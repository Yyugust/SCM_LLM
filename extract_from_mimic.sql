/* =============================================================================
   MIMIC-IV Sepsis Prediction Dataset (v6) with Antibiotics (Hourly Features)
   -----------------------------------------------------------------------------
   This script:
   1) Builds an ICU-stay cohort with LOS >= 8 hours and age 18-89.
   2) Extracts hourly vitals, labs, antibiotics exposure.
   3) Generates Sepsis-3 labels using derived sepsis3 table:
        - sepsis_time = min(suspected_infection_time, sofa_time)
        - label = 1 if t >= sepsis_time - 6 hours AND t >= icu_intime + 4 hours
   4) Writes a final table to mimiciv.public.

   Notes:
   - Designed for PostgreSQL with MIMIC-IV schemas:
       mimiciv_icu, mimiciv_hosp, mimiciv_derived
   - Uses materialized views for intermediate steps.
   - You may need appropriate privileges to CREATE in mimiciv.public.

============================================================================= */

-- =============================================================================
-- Step 1: Session-level PostgreSQL memory / parallel settings
-- =============================================================================
-- These SET commands apply to the current session only.
-- Adjust for your environment and available RAM.
SET work_mem = '';
SET maintenance_work_mem = '';
SET temp_buffers = '';
SET max_parallel_workers_per_gather = ;

-- =============================================================================
-- Step 2: Build base ICU-stay cohort (time constraints aligned to paper)
-- =============================================================================
DROP MATERIALIZED VIEW IF EXISTS tmp_icu_stays CASCADE;

CREATE MATERIALIZED VIEW tmp_icu_stays AS
SELECT
    i.stay_id,
    i.subject_id,
    i.hadm_id,
    i.intime AS icu_intime,
    i.outtime AS icu_outtime,
    CASE WHEN i.first_careunit LIKE '%MICU%' THEN 1 ELSE 0 END AS unit1,
    CASE WHEN i.first_careunit LIKE '%SICU%' THEN 1 ELSE 0 END AS unit2,
    EXTRACT(EPOCH FROM (i.outtime - i.intime)) / 3600 AS iculos,
    p.gender,
    p.anchor_age + EXTRACT(YEAR FROM a.admittime) - p.anchor_year AS age,
    EXTRACT(EPOCH FROM (i.intime - a.admittime)) / 3600 AS hospadmtime
FROM mimiciv_icu.icustays i
JOIN mimiciv_hosp.patients p
  ON i.subject_id = p.subject_id
JOIN mimiciv_hosp.admissions a
  ON i.hadm_id = a.hadm_id
WHERE
    -- ICU length-of-stay >= 8 hours
    EXTRACT(EPOCH FROM (i.outtime - i.intime)) / 3600 >= 8
    -- Age between 18 and 89 (inclusive)
    AND (p.anchor_age + EXTRACT(YEAR FROM a.admittime) - p.anchor_year) BETWEEN 18 AND 89;

CREATE INDEX idx_tmp_icu_stays_id
    ON tmp_icu_stays (stay_id);

CREATE INDEX idx_tmp_icu_stays_time
    ON tmp_icu_stays (stay_id, icu_intime, icu_outtime);

ANALYZE tmp_icu_stays;

-- =============================================================================
-- Step 3: Antibiotics (hourly exposure features)
-- =============================================================================
DROP MATERIALIZED VIEW IF EXISTS tmp_antibiotics CASCADE;

CREATE MATERIALIZED VIEW tmp_antibiotics AS
WITH antibiotic_hours AS (
    SELECT
        i.stay_id,
        a.antibiotic,
        a.starttime,
        a.stoptime,
        -- Generate hourly timestamps during antibiotic administration overlapping ICU stay.
        generate_series(
            DATE_TRUNC('hour', GREATEST(a.starttime, i.icu_intime)),
            DATE_TRUNC('hour', LEAST(COALESCE(a.stoptime, i.icu_outtime), i.icu_outtime)),
            INTERVAL '1 hour'
        ) AS hour
    FROM mimiciv_derived.antibiotic a
    JOIN tmp_icu_stays i
      ON a.stay_id = i.stay_id
    WHERE
        -- Ensure overlap between antibiotic interval and ICU stay
        a.starttime < i.icu_outtime
        AND COALESCE(a.stoptime, i.icu_outtime) > i.icu_intime
        -- Keep records roughly around ICU stay (±24h) as in the original logic
        AND a.starttime BETWEEN i.icu_intime - INTERVAL '24 hours'
                           AND i.icu_outtime + INTERVAL '24 hours'
)
SELECT
    stay_id,
    hour,
    COUNT(DISTINCT antibiotic) AS antibiotic_count,
    STRING_AGG(DISTINCT antibiotic, '; ') AS antibiotics_used,
    1 AS antibiotic_flag
FROM antibiotic_hours
GROUP BY stay_id, hour;

CREATE INDEX idx_tmp_antibiotics
    ON tmp_antibiotics (stay_id, hour);

ANALYZE tmp_antibiotics;

-- =============================================================================
-- Step 4: Vitals extraction with strict ICU time constraints (hourly medians)
-- =============================================================================

-- Part 1: Heart rate, SpO2, Temperature
DROP MATERIALIZED VIEW IF EXISTS tmp_vitals_part1 CASCADE;

CREATE MATERIALIZED VIEW tmp_vitals_part1 AS
SELECT
    c.stay_id,
    DATE_TRUNC('hour', c.charttime) AS hour,

    -- Heart rate (20-300)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN c.itemid = 220045 AND c.valuenum BETWEEN 20 AND 300
            THEN c.valuenum
        END
    ) AS hr,

    -- Oxygen saturation (0-100)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN c.itemid IN (220227, 220277) AND c.valuenum BETWEEN 0 AND 100
            THEN c.valuenum
        END
    ) AS o2sat,

    -- Temperature converted to Celsius, range (25-45°C)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN c.itemid = 223761 AND c.valuenum BETWEEN 86 AND 113
            THEN (c.valuenum - 32) * 5.0 / 9.0
            WHEN c.itemid = 223762 AND c.valuenum BETWEEN 30 AND 45
            THEN c.valuenum
        END
    ) AS temp
FROM mimiciv_icu.chartevents c
JOIN tmp_icu_stays i
  ON c.stay_id = i.stay_id
WHERE
    c.itemid IN (220045, 220227, 220277, 223761, 223762)
    AND c.valuenum IS NOT NULL
    -- Strictly within ICU stay
    AND c.charttime BETWEEN i.icu_intime AND i.icu_outtime
GROUP BY
    c.stay_id, DATE_TRUNC('hour', c.charttime);

-- Part 2: Blood pressure
DROP MATERIALIZED VIEW IF EXISTS tmp_vitals_part2 CASCADE;

CREATE MATERIALIZED VIEW tmp_vitals_part2 AS
SELECT
    c.stay_id,
    DATE_TRUNC('hour', c.charttime) AS hour,

    -- SBP (40-300)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN c.itemid IN (220050, 220179) AND c.valuenum BETWEEN 40 AND 300
            THEN c.valuenum
        END
    ) AS sbp,

    -- MAP (20-200)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN c.itemid IN (220051, 220180) AND c.valuenum BETWEEN 20 AND 200
            THEN c.valuenum
        END
    ) AS map,

    -- DBP (20-200)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN c.itemid IN (220052, 220181) AND c.valuenum BETWEEN 20 AND 200
            THEN c.valuenum
        END
    ) AS dbp
FROM mimiciv_icu.chartevents c
JOIN tmp_icu_stays i
  ON c.stay_id = i.stay_id
WHERE
    c.itemid IN (220050, 220179, 220051, 220180, 220052, 220181)
    AND c.valuenum IS NOT NULL
    -- Strictly within ICU stay
    AND c.charttime BETWEEN i.icu_intime AND i.icu_outtime
GROUP BY
    c.stay_id, DATE_TRUNC('hour', c.charttime);

-- Part 3: Respiratory rate, EtCO2, FiO2
DROP MATERIALIZED VIEW IF EXISTS tmp_vitals_part3 CASCADE;

CREATE MATERIALIZED VIEW tmp_vitals_part3 AS
SELECT
    c.stay_id,
    DATE_TRUNC('hour', c.charttime) AS hour,

    -- Respiratory rate (0-70)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN c.itemid IN (220210, 224690) AND c.valuenum BETWEEN 0 AND 70
            THEN c.valuenum
        END
    ) AS resp,

    -- EtCO2 (0-100)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN c.itemid = 228640 AND c.valuenum BETWEEN 0 AND 100
            THEN c.valuenum
        END
    ) AS etco2,

    -- FiO2 (21-100)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN c.itemid = 223835 AND c.valuenum BETWEEN 21 AND 100
            THEN c.valuenum
        END
    ) AS fio2
FROM mimiciv_icu.chartevents c
JOIN tmp_icu_stays i
  ON c.stay_id = i.stay_id
WHERE
    c.itemid IN (220210, 224690, 228640, 223835)
    AND c.valuenum IS NOT NULL
    -- Strictly within ICU stay
    AND c.charttime BETWEEN i.icu_intime AND i.icu_outtime
GROUP BY
    c.stay_id, DATE_TRUNC('hour', c.charttime);

-- Combine vitals
DROP MATERIALIZED VIEW IF EXISTS tmp_vitals_combined CASCADE;

CREATE MATERIALIZED VIEW tmp_vitals_combined AS
SELECT
    COALESCE(p1.stay_id, p2.stay_id, p3.stay_id) AS stay_id,
    COALESCE(p1.hour, p2.hour, p3.hour) AS hour,
    p1.hr,
    p1.o2sat,
    p1.temp,
    p2.sbp,
    p2.dbp,
    p2.map,
    p3.resp,
    p3.etco2,
    p3.fio2
FROM tmp_vitals_part1 p1
FULL OUTER JOIN tmp_vitals_part2 p2
  ON p1.stay_id = p2.stay_id AND p1.hour = p2.hour
FULL OUTER JOIN tmp_vitals_part3 p3
  ON COALESCE(p1.stay_id, p2.stay_id) = p3.stay_id
 AND COALESCE(p1.hour, p2.hour) = p3.hour;

CREATE INDEX idx_tmp_vitals_combined
    ON tmp_vitals_combined (stay_id, hour);

ANALYZE tmp_vitals_combined;

-- =============================================================================
-- Step 5: Labs extraction in batches (5 parts), hourly medians
-- =============================================================================

-- Part 1: Blood gas related
DROP MATERIALIZED VIEW IF EXISTS tmp_labs_part1 CASCADE;

CREATE MATERIALIZED VIEW tmp_labs_part1 AS
SELECT
    i.stay_id,
    DATE_TRUNC('hour', l.charttime) AS hour,

    -- Base excess (-30 to 30 mmol/L)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 50802 AND l.valuenum BETWEEN -30 AND 30
            THEN l.valuenum
        END
    ) AS baseexcess,

    -- Bicarbonate (5-50 mmol/L)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid IN (50803, 50882) AND l.valuenum BETWEEN 5 AND 50
            THEN l.valuenum
        END
    ) AS hco3,

    -- pH (6.8-8.0)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 50820 AND l.valuenum BETWEEN 6.8 AND 8.0
            THEN l.valuenum
        END
    ) AS ph,

    -- PaCO2 (10-120 mmHg)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 50818 AND l.valuenum BETWEEN 10 AND 120
            THEN l.valuenum
        END
    ) AS paco2
FROM mimiciv_hosp.labevents l
JOIN mimiciv_icu.icustays i
  ON l.hadm_id = i.hadm_id AND l.subject_id = i.subject_id
WHERE
    l.itemid IN (50802, 50882, 50803, 50820, 50818)
    AND l.valuenum IS NOT NULL
    AND l.charttime BETWEEN i.intime AND i.outtime
GROUP BY
    i.stay_id, DATE_TRUNC('hour', l.charttime);

CREATE INDEX idx_tmp_labs_part1
    ON tmp_labs_part1 (stay_id, hour);

-- Part 2: Liver/kidney function, etc.
DROP MATERIALIZED VIEW IF EXISTS tmp_labs_part2 CASCADE;

CREATE MATERIALIZED VIEW tmp_labs_part2 AS
SELECT
    i.stay_id,
    DATE_TRUNC('hour', l.charttime) AS hour,

    -- SaO2 (0-100%)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 50817 AND l.valuenum BETWEEN 0 AND 100
            THEN l.valuenum
        END
    ) AS sao2,

    -- AST (0-10000 IU/L)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 50878 AND l.valuenum BETWEEN 0 AND 10000
            THEN l.valuenum
        END
    ) AS ast,

    -- BUN (0-300 mg/dL)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 51006 AND l.valuenum BETWEEN 0 AND 300
            THEN l.valuenum
        END
    ) AS bun,

    -- Alkaline phosphatase (0-2000 IU/L)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 50863 AND l.valuenum BETWEEN 0 AND 2000
            THEN l.valuenum
        END
    ) AS alkalinephos,

    -- Calcium (4-20 mg/dL)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 50893 AND l.valuenum BETWEEN 4 AND 20
            THEN l.valuenum
        END
    ) AS calcium
FROM mimiciv_hosp.labevents l
JOIN mimiciv_icu.icustays i
  ON l.hadm_id = i.hadm_id AND l.subject_id = i.subject_id
WHERE
    l.itemid IN (50817, 50878, 51006, 50863, 50893)
    AND l.valuenum IS NOT NULL
    AND l.charttime BETWEEN i.intime AND i.outtime
GROUP BY
    i.stay_id, DATE_TRUNC('hour', l.charttime);

CREATE INDEX idx_tmp_labs_part2
    ON tmp_labs_part2 (stay_id, hour);

-- Part 3: Electrolytes and metabolic markers
DROP MATERIALIZED VIEW IF EXISTS tmp_labs_part3 CASCADE;

CREATE MATERIALIZED VIEW tmp_labs_part3 AS
SELECT
    i.stay_id,
    DATE_TRUNC('hour', l.charttime) AS hour,

    -- Chloride (70-150 mmol/L)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 50902 AND l.valuenum BETWEEN 70 AND 150
            THEN l.valuenum
        END
    ) AS chloride,

    -- Creatinine (0.1-25 mg/dL)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 50912 AND l.valuenum BETWEEN 0.1 AND 25
            THEN l.valuenum
        END
    ) AS creatinine,

    -- Direct bilirubin (0-50 mg/dL)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 50883 AND l.valuenum BETWEEN 0 AND 50
            THEN l.valuenum
        END
    ) AS bilirubin_direct,

    -- Glucose (10-1000 mg/dL)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 50931 AND l.valuenum BETWEEN 10 AND 1000
            THEN l.valuenum
        END
    ) AS glucose,

    -- Lactate (0-30 mmol/L)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 50813 AND l.valuenum BETWEEN 0 AND 30
            THEN l.valuenum
        END
    ) AS lactate
FROM mimiciv_hosp.labevents l
JOIN mimiciv_icu.icustays i
  ON l.hadm_id = i.hadm_id AND l.subject_id = i.subject_id
WHERE
    l.itemid IN (50902, 50912, 50883, 50931, 50813)
    AND l.valuenum IS NOT NULL
    AND l.charttime BETWEEN i.intime AND i.outtime
GROUP BY
    i.stay_id, DATE_TRUNC('hour', l.charttime);

CREATE INDEX idx_tmp_labs_part3
    ON tmp_labs_part3 (stay_id, hour);

-- Part 4: Electrolytes and cardiac markers
DROP MATERIALIZED VIEW IF EXISTS tmp_labs_part4 CASCADE;

CREATE MATERIALIZED VIEW tmp_labs_part4 AS
SELECT
    i.stay_id,
    DATE_TRUNC('hour', l.charttime) AS hour,

    -- Magnesium (0.5-5 mmol/L)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 50960 AND l.valuenum BETWEEN 0.5 AND 5
            THEN l.valuenum
        END
    ) AS magnesium,

    -- Phosphate (0.5-15 mg/dL)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 50970 AND l.valuenum BETWEEN 0.5 AND 15
            THEN l.valuenum
        END
    ) AS phosphate,

    -- Potassium (2-10 mmol/L)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 50971 AND l.valuenum BETWEEN 2 AND 10
            THEN l.valuenum
        END
    ) AS potassium,

    -- Total bilirubin (0-50 mg/dL)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 50885 AND l.valuenum BETWEEN 0 AND 50
            THEN l.valuenum
        END
    ) AS bilirubin_total,

    -- Troponin I (0-100 ng/mL)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid IN (51002, 52642) AND l.valuenum BETWEEN 0 AND 100
            THEN l.valuenum
        END
    ) AS troponini
FROM mimiciv_hosp.labevents l
JOIN mimiciv_icu.icustays i
  ON l.hadm_id = i.hadm_id AND l.subject_id = i.subject_id
WHERE
    l.itemid IN (50960, 50970, 50971, 50885, 51002, 52642)
    AND l.valuenum IS NOT NULL
    AND l.charttime BETWEEN i.intime AND i.outtime
GROUP BY
    i.stay_id, DATE_TRUNC('hour', l.charttime);

CREATE INDEX idx_tmp_labs_part4
    ON tmp_labs_part4 (stay_id, hour);

-- Part 5: Hematology
DROP MATERIALIZED VIEW IF EXISTS tmp_labs_part5 CASCADE;

CREATE MATERIALIZED VIEW tmp_labs_part5 AS
SELECT
    i.stay_id,
    DATE_TRUNC('hour', l.charttime) AS hour,

    -- Hematocrit (15-60%)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 51221 AND l.valuenum BETWEEN 15 AND 60
            THEN l.valuenum
        END
    ) AS hct,

    -- Hemoglobin (5-20 g/dL)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid IN (51222, 50811) AND l.valuenum BETWEEN 5 AND 20
            THEN l.valuenum
        END
    ) AS hgb,

    -- PTT (20-150 seconds)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 51275 AND l.valuenum BETWEEN 20 AND 150
            THEN l.valuenum
        END
    ) AS ptt,

    -- WBC (0.1-100 K/uL)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 51301 AND l.valuenum BETWEEN 0.1 AND 100
            THEN l.valuenum
        END
    ) AS wbc,

    -- Fibrinogen (50-1000 mg/dL)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 51279 AND l.valuenum BETWEEN 50 AND 1000
            THEN l.valuenum
        END
    ) AS fibrinogen,

    -- Platelets (10-1500 K/uL)
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY CASE
            WHEN l.itemid = 51265 AND l.valuenum BETWEEN 10 AND 1500
            THEN l.valuenum
        END
    ) AS platelets
FROM mimiciv_hosp.labevents l
JOIN mimiciv_icu.icustays i
  ON l.hadm_id = i.hadm_id AND l.subject_id = i.subject_id
WHERE
    l.itemid IN (51221, 51222, 50811, 51275, 51301, 51279, 51265)
    AND l.valuenum IS NOT NULL
    AND l.charttime BETWEEN i.intime AND i.outtime
GROUP BY
    i.stay_id, DATE_TRUNC('hour', l.charttime);

CREATE INDEX idx_tmp_labs_part5
    ON tmp_labs_part5 (stay_id, hour);

-- Combine labs
DROP MATERIALIZED VIEW IF EXISTS tmp_labs_combined CASCADE;

CREATE MATERIALIZED VIEW tmp_labs_combined AS
SELECT
    COALESCE(p1.stay_id, p2.stay_id, p3.stay_id, p4.stay_id, p5.stay_id) AS stay_id,
    COALESCE(p1.hour, p2.hour, p3.hour, p4.hour, p5.hour) AS hour,
    p1.baseexcess, p1.hco3, p1.ph, p1.paco2,
    p2.sao2, p2.ast, p2.bun, p2.alkalinephos, p2.calcium,
    p3.chloride, p3.creatinine, p3.bilirubin_direct, p3.glucose, p3.lactate,
    p4.magnesium, p4.phosphate, p4.potassium, p4.bilirubin_total, p4.troponini,
    p5.hct, p5.hgb, p5.ptt, p5.wbc, p5.fibrinogen, p5.platelets
FROM tmp_labs_part1 p1
FULL OUTER JOIN tmp_labs_part2 p2
  ON p1.stay_id = p2.stay_id AND p1.hour = p2.hour
FULL OUTER JOIN tmp_labs_part3 p3
  ON COALESCE(p1.stay_id, p2.stay_id) = p3.stay_id
 AND COALESCE(p1.hour, p2.hour) = p3.hour
FULL OUTER JOIN tmp_labs_part4 p4
  ON COALESCE(p1.stay_id, p2.stay_id, p3.stay_id) = p4.stay_id
 AND COALESCE(p1.hour, p2.hour, p3.hour) = p4.hour
FULL OUTER JOIN tmp_labs_part5 p5
  ON COALESCE(p1.stay_id, p2.stay_id, p3.stay_id, p4.stay_id) = p5.stay_id
 AND COALESCE(p1.hour, p2.hour, p3.hour, p4.hour) = p5.hour;

CREATE INDEX idx_tmp_labs_combined
    ON tmp_labs_combined (stay_id, hour);

ANALYZE tmp_labs_combined;

-- =============================================================================
-- Step 6: Sepsis labels (Sepsis-3 standard)
-- =============================================================================
DROP MATERIALIZED VIEW IF EXISTS tmp_sepsis_labels CASCADE;

CREATE MATERIALIZED VIEW tmp_sepsis_labels AS
SELECT
    s.stay_id,

    -- As defined: tsepsis is the earlier of suspected infection time and SOFA time.
    LEAST(s.suspected_infection_time, s.sofa_time) AS sepsis_time,

    s.sepsis3,

    -- Keep original timestamps for debugging/validation
    s.suspected_infection_time AS suspicion_time,
    s.sofa_time,

    -- Check if time window matches Sepsis-3 definition: tSOFA within [-24h, +12h] of tsuspicion
    CASE
        WHEN s.sofa_time BETWEEN s.suspected_infection_time - INTERVAL '24 hours'
                           AND s.suspected_infection_time + INTERVAL '12 hours'
        THEN TRUE
        ELSE FALSE
    END AS time_window_valid,

    -- Time difference in hours
    EXTRACT(EPOCH FROM (s.sofa_time - s.suspected_infection_time)) / 3600 AS time_diff_hours
FROM mimiciv_derived.sepsis3 s
JOIN tmp_icu_stays i
  ON s.stay_id = i.stay_id
WHERE
    s.sepsis3 = TRUE
    AND s.sofa_time IS NOT NULL
    AND s.suspected_infection_time IS NOT NULL
    -- Sepsis time occurs at least 4 hours after ICU admission (use the earlier time)
    AND LEAST(s.suspected_infection_time, s.sofa_time) >= i.icu_intime + INTERVAL '4 hours'
    -- Ensure Sepsis-3 time window: tSOFA within [-24h, +12h] of suspected infection time
    AND s.sofa_time BETWEEN s.suspected_infection_time - INTERVAL '24 hours'
                       AND s.suspected_infection_time + INTERVAL '12 hours';

CREATE INDEX idx_tmp_sepsis_labels
    ON tmp_sepsis_labels (stay_id, sepsis_time);

ANALYZE tmp_sepsis_labels;

-- =============================================================================
-- Step 7: Build final hourly dataset (including antibiotics)
-- =============================================================================
DROP TABLE IF EXISTS final_results CASCADE;

CREATE TEMP TABLE final_results AS
SELECT
    i.subject_id,
    i.hadm_id,
    i.stay_id,
    i.gender,
    i.age,
    i.unit1,
    i.unit2,
    i.hospadmtime,
    i.iculos,

    -- Use the hour from vitals or labs or antibiotics (whichever exists)
    COALESCE(v.hour, l.hour, a.hour) AS hour,

    -- Vitals
    v.hr,
    v.o2sat,
    v.temp,
    v.sbp,
    v.dbp,
    v.resp,
    v.etco2,
    COALESCE(
        v.map,
        CASE
            WHEN v.sbp IS NOT NULL AND v.dbp IS NOT NULL
            THEN (v.sbp + 2 * v.dbp) / 3
            ELSE NULL
        END
    ) AS map,
    v.fio2,

    -- Labs
    l.baseexcess,
    l.hco3,
    l.ph,
    l.paco2,
    l.sao2,
    l.ast,
    l.bun,
    l.alkalinephos,
    l.calcium,
    l.chloride,
    l.creatinine,
    l.bilirubin_direct,
    l.glucose,
    l.lactate,
    l.magnesium,
    l.phosphate,
    l.potassium,
    l.bilirubin_total,
    l.troponini,
    l.hct,
    l.hgb,
    l.ptt,
    l.wbc,
    l.fibrinogen,
    l.platelets,

    -- Antibiotics (hourly exposure)
    COALESCE(a.antibiotic_flag, 0) AS antibiotic_flag,
    COALESCE(a.antibiotic_count, 0) AS antibiotic_count,
    a.antibiotics_used,

    -- Sepsis label
    CASE
        WHEN s.stay_id IS NOT NULL
             -- Paper rule: label=1 if t >= tsepsis - 6 hours; else 0
             AND COALESCE(v.hour, l.hour, a.hour) >= s.sepsis_time - INTERVAL '6 hours'
             -- Additional safeguard: do not label positive before ICU_intime + 4 hours (avoid leakage)
             AND COALESCE(v.hour, l.hour, a.hour) >= i.icu_intime + INTERVAL '4 hours'
        THEN 1
        ELSE 0
    END AS sepsislabel
FROM tmp_icu_stays i
LEFT JOIN tmp_vitals_combined v
  ON i.stay_id = v.stay_id
 AND v.hour BETWEEN i.icu_intime AND i.icu_outtime
LEFT JOIN tmp_labs_combined l
  ON i.stay_id = l.stay_id
 AND (v.hour = l.hour OR (v.hour IS NULL AND l.hour BETWEEN i.icu_intime AND i.icu_outtime))
LEFT JOIN tmp_antibiotics a
  ON i.stay_id = a.stay_id
 AND COALESCE(v.hour, l.hour) = a.hour
LEFT JOIN tmp_sepsis_labels s
  ON i.stay_id = s.stay_id
WHERE
    COALESCE(v.hour, l.hour, a.hour) IS NOT NULL
    AND COALESCE(v.hour, l.hour, a.hour) BETWEEN i.icu_intime AND i.icu_outtime;

CREATE INDEX idx_final_results_id
    ON final_results (stay_id, hour);

CREATE INDEX idx_final_results_sepsis
    ON final_results (sepsislabel);

CREATE INDEX idx_final_results_antibiotic
    ON final_results (antibiotic_flag);

ANALYZE final_results;

-- =============================================================================
-- Step 8: Data quality checks (paper-aligned)
-- =============================================================================

-- 8.1 Validate sepsis label time logic
SELECT
    'Sepsis Label Time Logic Validation' AS check_type,
    COUNT(*) AS total_sepsis_hours,
    COUNT(DISTINCT stay_id) AS sepsis_stays,
    MIN(hour) AS earliest_positive_label,
    MAX(hour) AS latest_positive_label
FROM final_results
WHERE sepsislabel = 1;

-- 8.2 Check for any positive labels before ICU_intime + 4 hours (should be 0)
SELECT
    'Early Label Validation (Should be 0)' AS check_type,
    COUNT(*) AS invalid_early_labels
FROM final_results f
JOIN tmp_icu_stays i
  ON f.stay_id = i.stay_id
WHERE f.sepsislabel = 1
  AND f.hour < i.icu_intime + INTERVAL '4 hours';

-- 8.3 Antibiotic data validation summary
SELECT
    'Antibiotic Data Validation' AS check_type,
    COUNT(*) AS total_records,
    SUM(antibiotic_flag) AS records_with_antibiotics,
    COUNT(DISTINCT CASE WHEN antibiotic_flag = 1 THEN stay_id END) AS stays_with_antibiotics,
    ROUND(SUM(antibiotic_flag) * 100.0 / COUNT(*), 2) AS antibiotic_usage_rate_percent,
    MAX(antibiotic_count) AS max_concurrent_antibiotics
FROM final_results;

-- 8.4 Antibiotic vs sepsis label association
SELECT
    'Antibiotic-Sepsis Relationship' AS check_type,
    sepsislabel,
    COUNT(*) AS total_records,
    SUM(antibiotic_flag) AS records_with_antibiotics,
    ROUND(SUM(antibiotic_flag) * 100.0 / COUNT(*), 2) AS antibiotic_rate_percent
FROM final_results
GROUP BY sepsislabel
ORDER BY sepsislabel;

-- 8.5 Time window consistency within ICU stay
SELECT
    'Time Window Consistency' AS check_type,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN hour >= icu_intime AND hour <= icu_outtime THEN 1 END) AS valid_time_records,
    ROUND(
        COUNT(CASE WHEN hour >= icu_intime AND hour <= icu_outtime THEN 1 END) * 100.0 / COUNT(*),
        2
    ) AS time_consistency_percentage
FROM final_results f
JOIN tmp_icu_stays i
  ON f.stay_id = i.stay_id;

-- =============================================================================
-- Step 9: Create final persistent table + indexes + final reports
-- =============================================================================
DROP TABLE IF EXISTS mimiciv.public.mimiciv_sepsis_predictions_v6_with_antibiotics CASCADE;

CREATE TABLE mimiciv.public.mimiciv_sepsis_predictions_v6_with_antibiotics AS
SELECT *
FROM final_results
ORDER BY subject_id, stay_id, hour;

-- Indexes for downstream usage
CREATE INDEX idx_sepsis_v6_stay_id
    ON mimiciv.public.mimiciv_sepsis_predictions_v6_with_antibiotics (stay_id);

CREATE INDEX idx_sepsis_v6_hour
    ON mimiciv.public.mimiciv_sepsis_predictions_v6_with_antibiotics (hour);

CREATE INDEX idx_sepsis_v6_label
    ON mimiciv.public.mimiciv_sepsis_predictions_v6_with_antibiotics (sepsislabel);

CREATE INDEX idx_sepsis_v6_antibiotic
    ON mimiciv.public.mimiciv_sepsis_predictions_v6_with_antibiotics (antibiotic_flag);

CREATE INDEX idx_sepsis_v6_composite
    ON mimiciv.public.mimiciv_sepsis_predictions_v6_with_antibiotics (stay_id, hour, sepsislabel, antibiotic_flag);

ANALYZE mimiciv.public.mimiciv_sepsis_predictions_v6_with_antibiotics;

-- Final dataset summary
SELECT
    'Final Data Summary' AS report_type,
    COUNT(DISTINCT stay_id) AS total_stays,
    COUNT(*) AS total_records,
    SUM(sepsislabel) AS positive_labels,
    COUNT(DISTINCT CASE WHEN sepsislabel = 1 THEN stay_id END) AS sepsis_stays,
    ROUND(SUM(sepsislabel) * 100.0 / COUNT(*), 2) AS positive_rate_percent,
    SUM(antibiotic_flag) AS antibiotic_records,
    COUNT(DISTINCT CASE WHEN antibiotic_flag = 1 THEN stay_id END) AS antibiotic_stays,
    ROUND(SUM(antibiotic_flag) * 100.0 / COUNT(*), 2) AS antibiotic_rate_percent
FROM mimiciv.public.mimiciv_sepsis_predictions_v6_with_antibiotics;

-- Antibiotic usage details by concurrent antibiotic count
SELECT
    'Antibiotic Usage Details' AS report_type,
    antibiotic_count,
    COUNT(*) AS record_count,
    COUNT(DISTINCT stay_id) AS stay_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM mimiciv.public.mimiciv_sepsis_predictions_v6_with_antibiotics
WHERE antibiotic_flag = 1
GROUP BY antibiotic_count
ORDER BY antibiotic_count;

-- Top 10 antibiotic combinations
SELECT
    'Top Antibiotic Combinations' AS report_type,
    antibiotics_used,
    COUNT(*) AS usage_count,
    COUNT(DISTINCT stay_id) AS unique_stays,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM mimiciv.public.mimiciv_sepsis_predictions_v6_with_antibiotics
WHERE antibiotics_used IS NOT NULL
GROUP BY antibiotics_used
ORDER BY usage_count DESC
LIMIT 10;
