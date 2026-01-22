# =============================================================================
# Script overview                                                           ####
#
# This script builds a reproducible data pipeline for analyzing the availability,
# adoption, and agent involvement of supplemental crop insurance products in the
# U.S. Federal Crop Insurance Program (FCIP), with a focus on SCO and ECO.
#
# The workflow:
# 1. Initializes a standardized study environment (years, directories, seed).
# 2. Cleans and harmonizes RMA Summary of Business (SOB) data for add-on products.
# 3. Constructs annual Administrative Data Master (ADM) tables for SCO and ECO,
#    including expanded coverage variants (e.g., SCO-88 and SCO-90).
# 4. Builds agent-level datasets linking supplemental insurance availability and
#    adoption to licensed agents.
# 5. Produces a longitudinal panel capturing supplemental insurance offerings and
#    adoption patterns over time.
# 6. Serializes all intermediate and final datasets for reuse and reproducibility.
# 7. Optionally publishes cleaned data artifacts to a GitHub release for
#    transparent versioning and downstream research use.
#
# The script is designed to be rerunnable, fault-tolerant across years, and
# suitable for both local analysis and automated research workflows.
# =============================================================================
# Housekeeping                                                              ####
rm(list = ls(all = TRUE));gc()

# Load core dependencies:
# - data.table: fast data manipulation
# - rfcip: your FCIP/RMA helper toolkit (assumes installed)
library(data.table);library(rfcip)

# Directory paths for build artifacts and final serialized datasets
# NOTE: change these as needed for your local filesystem.
# - directory_data: where intermediate/cleaned datasets (.rds) are written
# - directory_output: where summary tables/figures/etc. are written
directory_data   <- "data-raw/releases/data"
directory_output <- "data-raw/releases/output"

# =============================================================================
# Initialize environment                                                    ####

# If running in a repo clone, document() is helpful. Otherwise, load the package.
# (This avoids devtools dependency when running outside the cloned repo.)

if (file.exists("DESCRIPTION")) {
  # Running inside a cloned repository
  # If devtools is missing, install it (used here for devtools::document()).
  if (!requireNamespace("devtools", quietly = TRUE)) {
    message("Installing 'devtools' (required for documentation)...")
    install.packages("devtools")
  }
  # Generate/update package documentation from roxygen comments (NAMESPACE, Rd files, etc.)
  devtools::document()

} else {
  # Running outside the repo clone
  # If the project package isn't installed locally, install from GitHub.
  if (!requireNamespace("fcipSupplementalLab", quietly = TRUE)) {
    message("Installing 'fcipSupplementalLab' from GitHub...")
    # remotes is used to install packages from GitHub without devtools.
    if (!requireNamespace("remotes", quietly = TRUE)) {
      install.packages("remotes")
    }

    # Install your package from GitHub (repo owner/name).
    remotes::install_github("ftsiboe/fcipSupplementalLab"
    )
  }
  # Attach the package so its functions are available.
  library(fcipSupplementalLab)
}

# tempdir() returns a session-specific temporary directory (good for scratch files).
temporary_dir <- tempdir()

# Create a structured "study_environment" object that carries:
# - the year window to analyze
# - random seed for reproducibility
# - project name
# - a set of local output directories to ensure exist / standardize outputs
study_environment <- setup_environment(
  year_beg = 2014,                                    # first study year (inclusive)
  year_end = as.numeric(format(Sys.Date(), "%Y")) - 1, # last study year (previous calendar year)
  seed = 1980632,                                     # RNG seed for reproducible sampling/splits
  project_name="fcipSupplementalLab",
  local_directories = list(
    file.path(directory_output,"summary"), # summaries (text / metadata / etc.)
    file.path(directory_output,"tables"),  # tabular outputs
    file.path(directory_output,"figures"), # figures/maps
    file.path(directory_data)              # serialized data outputs
  ))

study_environment$data_date <- Sys.Date()

# Persist the study_environment so later script sections can reload after cleanup.
saveRDS(study_environment,file = file.path(directory_data,"study_environment.rds"))

# Create a "keep list" of objects to survive subsequent rm(list=...) calls.
# Pattern: store Keep.List plus everything currently in the environment.
Keep.List<-c("Keep.List",ls())

# =============================================================================
# Clean and enrich RMA Summary of Business data                             ####

# Reset workspace EXCEPT objects in Keep.List, then run garbage collection.
rm(list= ls()[!(ls() %in% c(Keep.List))]);gc()

# Reload the study environment settings (years, paths, etc.)
study_environment <-readRDS(file.path(directory_data,"study_environment.rds"))

sob <- clean_rma_sobtpu(
  years = study_environment$year_beg:study_environment$year_end,
  insurance_plan = c(
    1:3, 90, # basic policy
    31:33,   # SCO
    87:89,   # ECO
    35:36,   # Stacked Inc Prot Plan
    16:17,   # MP
    67:69,   # MCO
    26:28,   # PACE
    37,      # HIP-WI
    38       # FIP-SI
    ),
  acres_only = TRUE,
  addon_only = TRUE,
  harmonize_insurance_plan_code = TRUE,
  harmonize_coverage_level_percent = FALSE,
  harmonize_unit_structure_code = FALSE)

# Compute/attach plan shares for supplemental plans (e.g., within-crop/county adoption shares).
sob <- get_supplemental_adoption(sob)

# Save the cleaned SOB dataset for reuse in later steps.
saveRDS(sob,file=file.path(directory_data,"cleaned_rma_sobtpu.rds"))

# =============================================================================
# Build SCO/ECO/Area ADM table (adds SCO88/SCO90)                           ####

# Reset workspace EXCEPT objects in Keep.List, then run garbage collection.
rm(list= ls()[!(ls() %in% c(Keep.List))]);gc()

# Reload the study environment settings
study_environment <-readRDS(file.path(directory_data,"study_environment.rds"))

# For each year from 2015 through the end of the study window:
# - build/clean the relevant ADM supplemental tables (SCO/ECO/Area)
# - write each year's cleaned table to an .rds file
# Using tryCatch ensures the loop continues even if a particular year fails.
lapply(
  2015:study_environment$year_end,
  function(year){
    tryCatch({
      saveRDS(clean_rma_sco_and_eco_adm(year),
              file=file.path(
                directory_data,
                paste0("cleaned_rma_adm_supplemental_",year,".rds")))
    }, error=function(e){NULL})
  })

# =============================================================================
# Clean agent-level data                                                    ####

# Reset workspace EXCEPT objects in Keep.List, then run garbage collection.
rm(list= ls()[!(ls() %in% c(Keep.List))]);gc()

# Reload study environment
study_environment <-readRDS(file.path(directory_data,"study_environment.rds"))

# For each year from 2015 through the end of the study window:
# - build agent-level datasets linked to supplemental adoption (SCO/ECO)
# Inputs:
# - relevant_adm: the cleaned ADM supplemental table for that year
# - relevant_sob: cleaned SOB restricted to that commodity year
# - keep_variables: retain only the indicators of interest (SCO, ECO90, ECO95)
# - temporary_dir: scratch dir for intermediate downloads/merges (if used internally)
# Output:
# - agentdata_<year>.rds per year
lapply(
  2015:study_environment$year_end,
  function(year){
    tryCatch({
      saveRDS(
        get_fcip_agents(
          year           = year,
          relevant_adm   = readRDS(paste0("data-raw/releases/data/cleaned_rma_adm_supplemental_",year,".rds")),
          relevant_sob   = readRDS(file.path(directory_data,"cleaned_rma_sobtpu.rds"))[commodity_year %in% year],
          keep_variables = c("sco", "eco90", "eco95"),
          temporary_dir  = tempdir()),
        file=file.path(
          directory_data,
          paste0("agentdata_",year,".rds")))
    }, error=function(e){NULL})
  })

# =============================================================================
# Build panel of supplemental insurance availability and adoption           ####

# Reset workspace EXCEPT objects in Keep.List, then run garbage collection.
rm(list= ls()[!(ls() %in% c(Keep.List))]);gc()

# Reload study environment
study_environment <- readRDS(file.path(directory_data,"study_environment.rds"))

# Build a panel dataset that combines:
# - availability (offerings)
# - adoption (uptake)
# of supplemental insurance products, based on cleaned SOB data.
df <- build_supplemental_adoption_dynamics(readRDS(file.path(directory_data,"cleaned_rma_sobtpu.rds")))

# Save the final panel dataset
saveRDS(df,file=file.path(directory_data,"supplemental_offering_and_adoption.rds"))

# =============================================================================
# GitHub release + upload block (wrapped in an anonymous function)          ####
# NOTE: As written, this function is DEFINED but not CALLED. To execute,
# you would need to call it, e.g., (function(){ ... })()

function(){

  # Optional quick auth sanity check: if gh is installed and configured,
  # gh_whoami() will return the authenticated user.
  if(requireNamespace("gh", quietly = TRUE)) try(gh::gh_whoami(), silent = TRUE)

  # create and uplaod data release
  # If a release with tag "data" already exists, delete it first (ignore errors).
  tryCatch({
    piggyback::pb_release_delete(repo = "ftsiboe/fcipSupplementalLab", tag = "data")
  }, error = function(e){NULL})

  # Create a GitHub Release named "Project Data" with tag "data".
  # The release body is read from a markdown file in data-raw/readme.
  piggyback::pb_release_create(
    repo = "ftsiboe/fcipSupplementalLab",
    tag  = "data",
    name = "Project Data",
    body = readr::read_file(file.path("data-raw/scripts","Releases_Project_Data.md")))

  # Initialize a local "new release" context for uploads (piggyback convenience helper).
  piggyback::pb_new_release( repo = "ftsiboe/fcipSupplementalLab", tag  = "data")

  # Upload all .rds files under ./data-raw/releases/data (recursively) as release assets.
  # overwrite = TRUE ensures reruns replace existing assets.
  piggyback::pb_upload(
    list.files("./data-raw/releases/data", full.names = TRUE, recursive = TRUE,pattern=".rds"),
    repo = "ftsiboe/fcipSupplementalLab", tag  = "data",overwrite = TRUE)

}
# =============================================================================

"study_environment.rds"
"cleaned_rma_sobtpu.rds"
"supplemental_offering_and_adoption.rds"

