# Hard reset of workspace
rm(list = ls(all = TRUE)); gc()

# Load required packages (assumes they’re installed & on library path)
library(data.table); library(rfcip)

# Initialize study environment
devtools::document()
study_environment <- setup_environment(
  year_beg = 2015, year_end = 2023, seed = 1980632,
  project_name="HiddenSafetynet2025",
  local_directories = list(
    file.path("data-raw", "output","aggregate_metrics"),
    file.path("data-raw", "output","summary"),
    file.path("data-raw", "output","figure_data"),
    file.path("data-raw", "output","figure"),
    file.path("data-raw", "scripts"),
    file.path("data", "cleaned_agents_data")
  ),
  fastscratch_directories=list("output/sims","output/expected","output/draw_farm"))

names(study_environment$wd)[names(study_environment$wd) %in% "sims"] <- "dir_sim"
names(study_environment$wd)[names(study_environment$wd) %in% "expected"] <- "dir_expected"
names(study_environment$wd)[names(study_environment$wd) %in% "draw_farm"] <- "dir_drawfarm"

saveRDS(study_environment,file ="data/study_environment.rds")

Keep.List<-c("Keep.List",ls())


# Clean and enrich RMA Summary of Business (SOB) data
rm(list= ls()[!(ls() %in% c(Keep.List))]);gc()
sob <- clean_rma_sobtpu(
  years = study_environment$year_beg:study_environment$year_end,
  insurance_plan = c(1:3, 31:33, 35:36, 87:89, 90),
  acres_only = TRUE,
  addon_only = TRUE,
  harmonize_insurance_plan_code = TRUE,
  harmonize_coverage_level_percent = FALSE,
  harmonize_unit_structure_code = FALSE)

sob <- clean_supplemental_plan_shares(sob)
saveRDS(sob,file ="data/cleaned_rma_sobtpu.rds")

# Build SCO/ECO/Area ADM table (adds SCO88/SCO90)
rm(list= ls()[!(ls() %in% c(Keep.List))]);gc()
data <- data.table::rbindlist(
  lapply(
    study_environment$year_beg:study_environment$year_end,
    clean_rma_sco_and_eco_adm
  ),
  fill = TRUE
)
saveRDS(data,file ="data/cleaned_rma_sco_and_eco_adm.rds")

# Clean agent-level data
rm(list= ls()[!(ls() %in% c(Keep.List))]);gc()
lapply(
  study_environment$year_beg:study_environment$year_end,
  clean_agents_data
)

# Build panel of supplemental insurance availability (offering) and adoption (acres)
rm(list= ls()[!(ls() %in% c(Keep.List))]);gc()
build_supplemental_offering_and_adoption(
    cleaned_rma_sobtpu_file_path = "data/cleaned_rma_sobtpu.rds",
    output_directory = "data")

