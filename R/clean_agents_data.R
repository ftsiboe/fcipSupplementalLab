#' Clean agent-level data for a given year
#'
#' Downloads, merges, and processes agent-level insurance data for the specified
#' `year`. Combines revenue draws, calibrated yields, and RMA reference data,
#' computes premium/subsidy measures, and saves the cleaned dataset as an RDS
#' file.
#'
#' @param year Integer. Commodity year to process (e.g., 2015).
#' @param cleaned_rma_sobtpu_file_path Path to cleaned RMA SOB/TPU RDS file.
#'   Default: `"data-raw/data/cleaned_rma_sobtpu.rds"`.
#' @param cleaned_rma_sco_and_eco_adm_file_path Path to cleaned RMA SCO & ECO
#'   admin RDS file. Default: `"data-raw/data/cleaned_rma_sco_and_eco_adm.rds"`.
#' @param output_directory Directory to save output RDS file. Created if missing.
#'   Default: `"data/cleaned_agents_data"`.
#'
#' @return Returns the input `year` on success, with attributes for `save_path`
#'   and number of rows. Returns `NULL` on error.
#'
#' @note Requires **data.table**, access to GitHub-hosted RDS files, and the
#'   helper function `get_compressed_adm()`.
#' @import data.table
#' @importFrom rfcip get_adm_data
#' @export
clean_agents_data <- function(
    year,
    cleaned_rma_sobtpu_file_path = "data/cleaned_rma_sobtpu.rds",
    cleaned_rma_sco_and_eco_adm_file_path = "data/cleaned_rma_sco_and_eco_adm.rds",
    output_directory = "data/cleaned_agents_data"){
  tryCatch({
    # --- 1) Download revenue draw data for the specified year ---
    agentdata <- tempfile(fileext = ".rds")
    download.file(
      paste0("https://github.com/ftsiboe/USFarmSafetyNetLab/releases/download/revenue_draw/revenue_draw_", year, ".rds"),
      agentdata, mode = "wb", quiet = TRUE)
    agentdata <- as.data.table(readRDS(agentdata))

    # --- 2) Restrict agentdata to relevant insurance pools found in SCO/ECO admin ---
    agentdata <- agentdata[
      unique(as.data.table(readRDS(cleaned_rma_sco_and_eco_adm_file_path))[
        commodity_year %in% year, c("state_code","county_code","commodity_code","type_code","practice_code"), with = FALSE]),
      on = c("state_code","county_code","commodity_code","type_code","practice_code"), nomatch = 0]; gc()

    # --- 3) Load SOB/TPU, filter by year, and build producer_id key ---
    rma_sob <- readRDS(cleaned_rma_sobtpu_file_path)[commodity_year %in% year]
    rma_sob[
      ,
      producer_id := do.call(paste, c(.SD, sep = "_")),
      .SDcols = c(
        "state_code", "county_code", "commodity_code", "type_code",
        "practice_code", "unit_structure_code", "insurance_plan_code",
        "coverage_type_code", "coverage_level_percent")]

    # --- 4) Join SOB/TPU into agentdata with explicit, preferred keys (safer than raw intersects) ---
    preferred_join_keys <- c("commodity_year","state_code","county_code","commodity_code","type_code","practice_code",
                             "producer_id","unit_structure_code","insurance_plan_code","coverage_type_code","coverage_level_percent")
    sob_join_keys <- intersect(preferred_join_keys, intersect(names(agentdata), names(rma_sob)))
    if (length(sob_join_keys) == 0L) stop("No overlapping join keys found between agentdata and rma_sob.")
    agentdata <- agentdata[rma_sob, on = sob_join_keys, nomatch = 0]; rm(rma_sob); gc()

    # --- 5) Compute observed premium metrics (robust to zero/NA denominators) ---
    agentdata[, observed_premium_rate := {
      denom <- liability_amount
      num   <- total_premium_amount
      val   <- ifelse(is.finite(denom) & !is.na(denom) & denom > 0, num / denom, NA_real_)
      round(val, 8)
    }]
    agentdata[, observed_subsidy_percent := {
      denom <- total_premium_amount
      num   <- subsidy_amount
      val   <- ifelse(is.finite(denom) & !is.na(denom) & denom > 0, num / denom, NA_real_)
      round(val, 3)
    }]

    # --- 6) Download calibrated yield data for the specified year ---
    calibrated_yield <- tempfile(fileext = ".rds")
    download.file(
      paste0("https://github.com/ftsiboe/USFarmSafetyNetLab/releases/download/calibrated_yield/calibrated_yield_", year, ".rds"),
      calibrated_yield, mode = "wb", quiet = TRUE)

    calibrated_yield <- as.data.table(readRDS(calibrated_yield))[
      , c("producer_id", "rate_yield", "approved_yield", "calibrated_yield"), with = FALSE]

    # --- 7) Join calibrated yield columns into agentdata using explicit keys when possible ---
    cy_join_keys <- intersect(c("commodity_year","state_code","county_code","commodity_code","type_code","practice_code", "producer_id"),
                              intersect(names(agentdata), names(calibrated_yield)))
    if (length(cy_join_keys) == 0L) stop("No overlapping join keys found between agentdata and calibrated_yield.")
    agentdata <- agentdata[calibrated_yield, on = cy_join_keys, nomatch = 0]; rm(calibrated_yield); gc()

    # --- 8) Attach projected price (mean by year and FCIP pool) via data.table update join ---
    price <- tempfile(fileext = ".rds")
    download.file(
      paste0("https://github.com/ftsiboe/USFarmSafetyNetLab/releases/download/adm_compressed/",year,"_A00810_Price.rds"),
      price, mode = "wb", quiet = TRUE)
    price <- as.data.table(readRDS(price))[
      , lapply(.SD, function(x) mean(x, na.rm = TRUE)),
      by = c("commodity_year", "state_code","county_code","commodity_code","type_code","practice_code"),
      .SDcols = c("projected_price")]
    agentdata <- merge(agentdata, price, by = intersect(names(agentdata), names(price)), all = TRUE);rm(price)

    # --- 9) Filter out invalid values for calibrated_yield and observed ratios; select columns ---
    agentdata <- agentdata[
      is.finite(calibrated_yield) &
        (is.na(observed_premium_rate)        | is.finite(observed_premium_rate)) &
        (is.na(observed_subsidy_percent)     | is.finite(observed_subsidy_percent)),
      c("commodity_year", "state_code","county_code","commodity_code","type_code","practice_code" ,
        "unit_structure_code","insurance_plan_code","coverage_type_code","coverage_level_percent", "producer_id",
        "rate_yield", "approved_yield", "calibrated_yield", "projected_price",
        "observed_premium_rate", "observed_subsidy_percent", "insured_acres", "sco", "eco90", "eco95",
        "rma_draw_lookup_rate", "rma_draw_number", "rma_draw_yield_farm",
        "rma_draw_price_farm", "rma_draw_yield_pool", "rma_draw_price_pool"),
      with = FALSE]

    # --- 10) Add downstream convenience columns; fix name + provide backward-compatible alias ---
    agentdata[, planted_acres      := insured_acres]
    agentdata[, price_election     := 1]
    agentdata[, insured_share      := 1]
    agentdata[, damage_area_rate   := 1]

    # --- 11) Save output and return informative attributes while keeping return type ---
    if (!dir.exists(output_directory)) dir.create(output_directory, recursive = TRUE)
    save_path <- file.path(output_directory, paste0("cleaned_agents_data_", year, ".rds"))
    saveRDS(as.data.table(agentdata), file = save_path)
    n_rows <- nrow(agentdata)
    rm(agentdata); gc()

    # Keep return value (year) but attach useful attributes for discoverability
    result <- structure(year, save_path = save_path, n_rows = n_rows)
    return(result)
  }, error = function(e){ return(NULL) })
}

