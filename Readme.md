# SCM_LLM

> **Status:** Currently updating

## Communication

If you need any data or code from this project, please feel free to communicate with us. We will organize and release the requested materials in a timely manner.

## Datasets

### 1. Early Prediction of Sepsis from Clinical Data
**Challenge:** PhysioNet/Computing in Cardiology Challenge 2019  
**Source:** [PhysioNet](https://physionet.org/content/challenge-2019/1.0.0/)

### 2. MIMIC-IV Database
**Version:** v3.1  
**Source:** [PhysioNet MIMIC-IV](https://physionet.org/content/mimiciv/3.1/)
**Extraction (Challenge 2019-aligned)**

To ensure feature definitions are consistent with the PhysioNet/Computing in Cardiology Challenge 2019 (Early Prediction of Sepsis), we provide `extract_from_mimic.sql` to extract and construct variables from MIMIC-IV following the same (or directly mappable) standards used in the Challenge 2019 dataset. This includes aligning variable names, units, and time aggregation as closely as possible to the Challenge specification to facilitate reproducibility and fair comparison.

- SQL script: `extract_from_mimic.sql`
- Purpose: extract and generate Challenge 2019-aligned features from MIMIC-IV
- Example usage (PostgreSQL):
  ```sql
  \i extract_from_mimic.sql

