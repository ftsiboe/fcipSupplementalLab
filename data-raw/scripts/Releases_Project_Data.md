
<div style="font-size: 0.75em; line-height: 1.25;">

⚠️ <strong>Disclaimer:</strong> These data are provided <strong>“as
is”</strong>, without warranty of any kind. The authors make no
guarantees regarding accuracy or suitability for any purpose. Use of
these data and any results derived from them is entirely at the user’s
own risk. The contents of this release reflect independent research and
do not represent the official positions or policies of the USDA, the
Risk Management Agency (RMA), the Federal Crop Insurance Corporation
(FCIC), or any Approved Insurance Provider.

</div>

------------------------------------------------------------------------

## 📘 Overview

This release provides **analysis-ready datasets** supporting research on
supplemental crop insurance products within the **U.S. Federal Crop
Insurance Program (FCIP)**.

The data are generated and maintained by the `fcipSupplementalLab`
workflow and are designed to support:

- Measurement of supplemental insurance availability across crops and
  regions
- Analysis of adoption patterns over time
- Agent-level participation analysis for upplemental crop insurance
  products
- Policy analysis related to FCIP subsidy reforms and program design

All files are serialized as `.rds` objects and intended for **direct use
in R**. Most users do **not** need to regenerate these data.

------------------------------------------------------------------------

## 🔁 Reproducibility

All datasets in this release were generated using the build script:

**`data-raw/scripts/001_workflow01_generate_fcipSupplementalLab_data_releases.R`**

The script:

- Initializes a standardized study environment
- Clears the R workspace between major build stages
- Iterates safely across years with fault tolerance
- Produces versioned `.rds` artifacts suitable for reuse and release

Users who wish to **rebuild or extend the datasets** (e.g., to update
the study period or modify filters) may run the script locally. Output
paths can be customized within the script.

> **Note:** The script is designed for local execution and intentionally
> resets the workspace between steps to avoid cross-contamination across
> data products.

------------------------------------------------------------------------

## 📁 Included Files

### **1) Reproducible study environment**

A serialized study-environment object created by `setup_environment()`
to standardize the data build across machines and runs. The object
records:

- Analysis window (`year_beg`, `year_end`)
- Fixed random seed
- Project name
- Required local directory structure

This file is reloaded by subsequent build stages to ensure
reproducibility.

**File:**  
`study_environment.rds`

------------------------------------------------------------------------

### **2) Cleaned RMA Summary of Business data (supplemental plans)**

An analysis-ready **SOB-TPU** dataset restricted to **add-on /
supplemental insurance products**, harmonized across years and coding
schemes.

Key features: - Acres-only records - Supplemental insurance plans
(including SCO and ECO) - Harmonized insurance plan codes - Supplemental
plan share calculations

This dataset is the backbone for adoption, availability, and agent-level
analysis.

**File:**  
`cleaned_rma_sobtpu.rds`

------------------------------------------------------------------------

### **3) Annual ADM tables for SCO and ECO**

A collection of **year-specific Actuarial Data Master (ADM)** extracts
capturing supplemental insurance availability, including expanded
coverage variants (e.g., SCO-88 and SCO-90).

Each file corresponds to a single commodity year.

**Files:**  
`cleaned_rma_adm_supplemental_<year>.rds`  
(e.g., `cleaned_rma_adm_supplemental_2023.rds`)

------------------------------------------------------------------------

### **4) Agent-level supplemental insurance datasets**

Annual agent-level datasets linking licensed FCIP agents to:

- SCO availability
- ECO 90 percent participation
- ECO 95 percent participation

Agent records are constructed by merging cleaned ADM data with SOB
adoption information for the corresponding year.

**Files:**  
`agentdata_<year>.rds`  
(e.g., `agentdata_2022.rds`)

------------------------------------------------------------------------

### **5) Panel of supplemental insurance availability and adoption**

A longitudinal panel dataset capturing:

- Whether SCO and ECO products are offered
- Whether they are adopted
- How availability and uptake evolve over time

This dataset is designed for descriptive analysis, econometric modeling,
and policy evaluation.

**File:**  
`supplemental_offering_and_adoption.rds`

------------------------------------------------------------------------

## 📦 Data Access and Versioning

All datasets are distributed via a **GitHub Release** tagged `data` and
uploaded using the `piggyback` package. This ensures:

- Stable download URLs
- Transparent versioning
- Reproducibility for collaborators and reviewers

------------------------------------------------------------------------

## 📌 Intended Use

These data are intended for:

- Research on supplemental crop insurance design and adoption
- Policy analysis under current and proposed FCIP reforms
- Internal ARPC and academic workflows
- Replication and extension by external researchers

They are **not official USDA or RMA products** and should not be
interpreted as such.

------------------------------------------------------------------------

⭐ *If you find this data release useful, please consider starring the
repository.*
