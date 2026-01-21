#' Build FCIP record-level dataset for a commodity year from calibration artifacts and RMA reference tables
#'
#' Downloads year-specific calibration artifacts from GitHub (revenue draws,
#' calibrated yields, and compressed projected prices), restricts revenue-draw
#' records to insurance pools present in `relevant_adm`, joins SOB/TPU reference
#' records from `relevant_sob` using explicit and validated keys, computes observed
#' premium-rate and subsidy-share measures, attaches yield and price fields,
#' filters invalid records, and returns a streamlined `data.table`.
#'
#' @param year Integer. Commodity year to process (e.g., 2015).
#' @param relevant_adm data.table. Pre-filtered administrative table defining the
#'   set of relevant insurance pools. Must contain at least
#'   `state_code`, `county_code`, `commodity_code`, `type_code`, and
#'   `practice_code`.
#' @param relevant_sob data.table. Pre-filtered SOB/TPU-style table used to
#'   construct `producer_id` and to join reference records into the revenue-draw
#'   data. Must contain the fields required to build `producer_id`:
#'   `state_code`, `county_code`, `commodity_code`, `type_code`, `practice_code`,
#'   `unit_structure_code`, `insurance_plan_code`, `coverage_type_code`,
#'   `coverage_level_percent`. If present, `commodity_year` may also be used
#'   in joins.
#' @param keep_variables Character vector of additional column names to retain
#'   (if present after all joins and filtering). Default is `NULL`.
#' @param temporary_dir Character. Directory used to store downloaded calibration
#'   artifacts. Defaults to `tempdir()`. The directory will be created if it does
#'   not exist.
#'
#' @details
#' The function expects calibration artifacts to be available as GitHub release
#' assets with the following structure:
#'
#' \itemize{
#'   \item Repository `ftsiboe/rfcipCalibrate`, tag `revenue_draw`:
#'     `revenue_draw_<year>.rds`
#'   \item Repository `ftsiboe/rfcipCalibrate`, tag `calibrated_yield`:
#'     `calibrated_yield_<year>.rds`
#'   \item Repository `ftsiboe/rfcipCalcPass`, tag `adm_compressed`:
#'     `<year>_A00810_Price.rds`
#' }
#'
#' Revenue-draw records are first restricted to insurance pools observed in
#' `relevant_adm`. SOB/TPU records are then joined **into** the revenue-draw data
#' (inner join), ensuring the unit of observation remains the revenue-draw /
#' policy-unit record.
#'
#' Observed ratios are computed using NA-safe denominators:
#' \itemize{
#'   \item `observed_premium_rate = total_premium_amount / liability_amount`
#'     (rounded to 8 decimals)
#'   \item `observed_subsidy_percent = subsidy_amount / total_premium_amount`
#'     (rounded to 3 decimals)
#' }
#' Denominators that are non-finite, `NA`, or non-positive yield `NA_real_`.
#'
#' Projected prices are aggregated to the mean by
#' `commodity_year`, `state_code`, `county_code`, `commodity_code`, `type_code`,
#' and `practice_code`, and are left-joined into the output so no additional rows
#' are created.
#'
#' The function filters out records with non-finite `calibrated_yield` values.
#' Records with missing observed ratios are retained.
#'
#' Convenience columns are added for downstream FCIP pipelines:
#' `planted_acres = insured_acres`, and `price_election`, `insured_share`, and
#' `damage_area_rate` are set to 1.
#'
#' @return
#' A `data.table` containing one row per retained FCIP record, including identifying
#' keys, selected calibration and draw fields, observed premium and subsidy
#' measures, projected price, and any variables listed in `keep_variables` that
#' exist. Returns `NULL` if an error occurs.
#'
#' @section Side effects and requirements:
#' \itemize{
#'   \item Downloads external files using `piggyback::pb_download()`.
#'   \item Reads and writes temporary RDS files in `temporary_dir`.
#'   \item Requires the calibration artifacts to be accessible via GitHub releases.
#' }
#'
#' @import data.table
#' @export
get_fcip_agents <- function(
    year,
    relevant_adm,
    relevant_sob,
    keep_variables = NULL,
    temporary_dir = tempdir()
){
  tryCatch({
    
    stopifnot(length(year) == 1L, is.finite(year))
    year <- as.integer(year)
    
    if (!dir.exists(temporary_dir)) dir.create(temporary_dir, recursive = TRUE, showWarnings = FALSE)
    
    # ------------------------------------------------------------------
    # 1) Download + read revenue draw data
    # ------------------------------------------------------------------
    rev_file <- paste0("revenue_draw_", year, ".rds")
    
    piggyback::pb_download(
      file      = rev_file,
      dest      = temporary_dir,
      repo      = "ftsiboe/rfcipCalibrate",
      tag       = "revenue_draw",
      overwrite = TRUE
    )
    
    agentdata <- readRDS(file.path(temporary_dir, rev_file))
    data.table::setDT(agentdata)
    
    # ------------------------------------------------------------------
    # 2) Restrict to relevant pools from relevant_adm
    # ------------------------------------------------------------------
    pool_keys <- c("state_code","county_code","commodity_code","type_code","practice_code")
    missing_pool <- setdiff(pool_keys, names(relevant_adm))
    if (length(missing_pool) > 0L) {
      stop("relevant_adm missing required columns: ", paste(missing_pool, collapse = ", "))
    }
    
    agentdata <- agentdata[
      unique(data.table::as.data.table(relevant_adm)[, ..pool_keys]),
      on = pool_keys,
      nomatch = 0L
    ]
    
    # ------------------------------------------------------------------
    # 3) Prep SOB/TPU and ensure producer_id exists
    # ------------------------------------------------------------------
    rma_sob <- data.table::copy(data.table::as.data.table(relevant_sob))
    
    pid_cols <- c(
      "state_code","county_code","commodity_code","type_code","practice_code",
      "unit_structure_code","insurance_plan_code","coverage_type_code","coverage_level_percent"
    )
    missing_pid <- setdiff(pid_cols, names(rma_sob))
    if (length(missing_pid) > 0L) {
      stop("relevant_sob missing columns needed to build producer_id: ", paste(missing_pid, collapse = ", "))
    }
    
    # Build producer_id in SOB
    rma_sob[, producer_id := do.call(paste, c(.SD, sep = "_")), .SDcols = pid_cols]
    
    # Require producer_id in agentdata (don't silently join on weaker keys)
    if (!"producer_id" %in% names(agentdata)) {
      stop("agentdata (revenue_draw) is missing `producer_id`; cannot safely join SOB/TPU.")
    }
    
    # ------------------------------------------------------------------
    # 4) Join SOB -> agentdata (KEEP agentdata rows; INNER join)
    # ------------------------------------------------------------------
    preferred_join_keys <- c(
      "commodity_year","state_code","county_code","commodity_code","type_code","practice_code",
      "producer_id","unit_structure_code","insurance_plan_code","coverage_type_code","coverage_level_percent"
    )
    sob_join_keys <- intersect(preferred_join_keys, intersect(names(agentdata), names(rma_sob)))
    if (length(sob_join_keys) == 0L) stop("No overlapping join keys found between agentdata and relevant_sob.")
    
    # (Optional safety) warn on duplicate SOB keys, which can explode row counts
    if (anyDuplicated(rma_sob[, ..sob_join_keys]) > 0L) {
      warning("Duplicate keys detected in relevant_sob on join fields; join may duplicate agent records.")
    }
    
    # Keep agentdata rows that match SOB; bring SOB cols
    agentdata <- rma_sob[agentdata, on = sob_join_keys, nomatch = 0L]
    rm(rma_sob); gc()
    
    # ------------------------------------------------------------------
    # 5) Compute observed premium metrics (NA-safe)
    # ------------------------------------------------------------------
    agentdata[, observed_premium_rate := {
      denom <- liability_amount
      num   <- total_premium_amount
      val   <- data.table::fifelse(is.finite(denom) & !is.na(denom) & denom > 0, num / denom, NA_real_)
      round(val, 8)
    }]
    
    agentdata[, observed_subsidy_percent := {
      denom <- total_premium_amount
      num   <- subsidy_amount
      val   <- data.table::fifelse(is.finite(denom) & !is.na(denom) & denom > 0, num / denom, NA_real_)
      round(val, 3)
    }]
    
    # ------------------------------------------------------------------
    # 6) Download + read calibrated yields
    # ------------------------------------------------------------------
    cy_file <- paste0("calibrated_yield_", year, ".rds")
    
    piggyback::pb_download(
      file      = cy_file,
      dest      = temporary_dir,
      repo      = "ftsiboe/rfcipCalibrate",
      tag       = "calibrated_yield",
      overwrite = TRUE
    )
    
    calibrated_yield <- readRDS(file.path(temporary_dir, cy_file))
    data.table::setDT(calibrated_yield)
    
    need_cy <- c("producer_id","rate_yield","approved_yield","calibrated_yield")
    missing_cy <- setdiff(need_cy, names(calibrated_yield))
    if (length(missing_cy) > 0L) stop("calibrated_yield file missing columns: ", paste(missing_cy, collapse = ", "))
    
    calibrated_yield <- calibrated_yield[, ..need_cy]
    
    # ------------------------------------------------------------------
    # 7) Join calibrated yields into agentdata (KEEP agentdata rows; INNER join)
    # ------------------------------------------------------------------
    cy_join_keys <- intersect(
      c("commodity_year","state_code","county_code","commodity_code","type_code","practice_code","producer_id"),
      intersect(names(agentdata), names(calibrated_yield))
    )
    if (length(cy_join_keys) == 0L) stop("No overlapping join keys found between agentdata and calibrated_yield.")
    
    if (anyDuplicated(calibrated_yield[, ..cy_join_keys]) > 0L) {
      warning("Duplicate keys detected in calibrated_yield on join fields; join may duplicate agent records.")
    }
    
    agentdata <- calibrated_yield[agentdata, on = cy_join_keys, nomatch = 0L]
    rm(calibrated_yield); gc()
    
    # ------------------------------------------------------------------
    # 8) Download + attach projected price (mean by pool)
    # ------------------------------------------------------------------
    price_file <- paste0(year, "_A00810_Price.rds")
    
    piggyback::pb_download(
      file      = price_file,
      dest      = temporary_dir,
      repo      = "ftsiboe/rfcipCalcPass",
      tag       = "adm_compressed",
      overwrite = TRUE
    )
    
    price <- readRDS(file.path(temporary_dir, price_file))
    data.table::setDT(price)
    
    price_keys <- c("commodity_year","state_code","county_code","commodity_code","type_code","practice_code")
    missing_price_keys <- setdiff(price_keys, names(price))
    if (length(missing_price_keys) > 0L) stop("price file missing key columns: ", paste(missing_price_keys, collapse = ", "))
    if (!"projected_price" %in% names(price)) stop("price file missing `projected_price`.")
    
    price <- price[
      , .(projected_price = mean(projected_price, na.rm = TRUE)),
      by = price_keys
    ]
    
    # Left-join price onto agentdata (do NOT create extra rows)
    join_price_keys <- intersect(price_keys, names(agentdata))
    agentdata <- price[agentdata, on = join_price_keys]
    rm(price); gc()
    
    # ------------------------------------------------------------------
    # 9) Filter invalids + select columns
    # ------------------------------------------------------------------
    keep <- unique(c(
      "commodity_year","state_code","county_code","commodity_code","type_code","practice_code",
      "unit_structure_code","insurance_plan_code","coverage_type_code","coverage_level_percent","producer_id",
      "rate_yield","approved_yield","calibrated_yield","projected_price",
      "observed_premium_rate","observed_subsidy_percent","insured_acres",
      "rma_draw_lookup_rate","rma_draw_number","rma_draw_yield_farm",
      "rma_draw_price_farm","rma_draw_yield_pool","rma_draw_price_pool",
      keep_variables
    ))
    keep <- intersect(keep, names(agentdata))
    
    agentdata <- agentdata[
      is.finite(calibrated_yield) &
        (is.na(observed_premium_rate)    | is.finite(observed_premium_rate)) &
        (is.na(observed_subsidy_percent) | is.finite(observed_subsidy_percent)),
      ..keep
    ]
    
    # ------------------------------------------------------------------
    # 10) Convenience columns
    # ------------------------------------------------------------------
    if ("insured_acres" %in% names(agentdata)) {
      agentdata[, planted_acres := insured_acres]
    } else {
      agentdata[, planted_acres := NA_real_]
    }
    agentdata[, price_election   := 1]
    agentdata[, insured_share    := 1]
    agentdata[, damage_area_rate := 1]
    
    agentdata
    
  }, error = function(e){
    NULL
  })
}


