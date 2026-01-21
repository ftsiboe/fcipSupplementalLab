#' Compute expected outcomes and risk metrics from simulation outputs
#'
#' Joins cleaned agent records to simulation files, then computes expected
#' (mean/sd) revenues, downside-risk measures (loss-side residual moments),
#' relative improvements with insurance, and insurance performance statistics.
#' Writes a single `.rds` result file and returns its path (invisibly).
#'
#' @param year Integer (scalar). Analysis year (used to resolve input/output paths).
#' @param task_id Integer or integer vector. Pseudo-task partition(s) to keep; the
#'   function cycles a 1..500 index over agent rows and filters to these values.
#' @param agents_directory Character. Directory containing `cleaned_agents_data_<year>.rds`.
#' @param simulation_directory Character or `NULL`. Directory with simulation `.rds` files;
#'   default is `file.path(study_environment$wd$dir_sim, year)`.
#' @param output_directory Character or `NULL`. Directory to write results;
#'   default is `file.path(study_environment$wd$dir_expected, year)`.
#' @param study_environment List. Must include `wd$dir_sim` and `wd$dir_expected` if
#'   the corresponding directory arguments are `NULL`.
#' @param agent_identifiers Character vector. Columns that identify agent units and
#'   define aggregation groups (used for joins and `by`); default includes year,
#'   location, crop, unit structure, plan, coverage, and acres.
#' @param disaggregate Character or `NULL`. Optional extra column to disaggregate by
#'   (for example, `"combination"`). If provided but missing after the join, the column
#'   is created and set to `"ALL"`.
#'
#' @details
#' **Pipeline**
#' 1. Load agent data and keep only `agent_identifiers`; coerce to `data.table`.
#' 2. Assign a pseudo `task` (cycles 1..500), then filter to `task_id`.
#' 3. Guardrails:
#'    - Stop if no simulation files are found.
#'    - Stop if the combined join yields zero rows.
#'    - Validate required numeric columns: `revenue`, `indemnity`, `producer_premium`,
#'      `liability`, `total_premium`, `subsidy_amount`.
#'    - Use `safe_div()` to avoid `Inf`/`NaN` on zero or non-finite denominators.
#' 4. Compute revenues (floored at 0): `Revenue` and `Revenue_Inc` (= revenue + indemnity
#'    - producer premium).
#' 5. By `uid` (=`agent_identifiers` plus `disaggregate` if provided), compute means,
#'    sds, residual-based downside measures (loss-only squared residuals and their
#'    frequency), and derived statistics (variance, CV, LAPV, LRPV, normalized forms).
#' 6. Compute **relative** metrics (insured vs. uninsured ratios): `Relmean`, `Relsd`,
#'    `Relcv`, `Rellapv`, `Rellrpv`, `Relnlapv`, `Relnlrpv`, `Relvar`. Base `Revenue*`
#'    statistics are dropped before the final merge to keep results compact.
#' 7. Aggregate insurance performance by group: mean `liability`, `total_premium`,
#'    `subsidy_amount`, `producer_premium`, `indemnity`, premium and LCR rates
#'    (`Simrate`, `SimrateP`, `Simsuby`, `Simlcr`), and **group sums** for
#'    `lr_indemnity` and `lr_premium`. Merge with the relative metrics.
#'
#' **Join note**
#' The join uses `data[simdt, on = <keys>, nomatch = 0]`, i.e., it returns rows
#' aligned to the simulation table entries that match the agent keys.
#'
#' @return Invisibly returns the saved file path (`expected_<year>_<task-range>.rds`).
#' @import data.table
#' @importFrom stats sd
#' @export
compute_expected_outcomes <- function(
    year,
    task_id,
    agents_directory = "data/cleaned_agents_data",
    simulation_directory = NULL,
    output_directory = NULL,
    study_environment,
    agent_identifiers = c("commodity_year","state_code","county_code","commodity_code","type_code","practice_code",
                          "unit_structure_code","insurance_plan_code","coverage_level_percent","insured_acres"),
    disaggregate=NULL){

  # base grouping keys
  uid <- agent_identifiers

  # optionally add an extra disaggregation level
  if(!is.null(disaggregate)){
    uid <-  c(uid,disaggregate)
  }

  # Helpers
  safe_div <- function(num, den) {
    # Safe division: returns NA when denominator is 0 / non-finite
    res <- num / den
    res[!is.finite(res) | den == 0] <- NA_real_
    res
  }
  is_bad <- function(x) !is.finite(x) # marks Inf, -Inf, NaN

  # Resolve directories from study_environment if not provided
  if (is.null(simulation_directory)) {
    simulation_directory <- file.path(study_environment$wd$dir_sim, year)
  }
  if (is.null(output_directory)) {
    output_directory <- file.path(study_environment$wd$dir_expected, year)
  }

  # Load agents
  # Keep only identifier columns; everything else will come from simulations
  data <- readRDS(paste0(agents_directory,"/cleaned_agents_data_",year,".rds"))[,agent_identifiers, with = FALSE]
  data <- data.table::as.data.table(data)

  # Tasking (pseudo)
  # Cycles 1..500 across rows to create a simple partitioning; then filter
  data[, task := rep(1:500, length.out = .N)]
  data <- data[task %in% task_id]

  # Read+join Simulation (keep AGENT rows)
  # Reads FIRST 5 files from simulation_directory; joins on core keys; binds all
  sim_files <- list.files(simulation_directory, full.names = TRUE)
  if (length(sim_files) == 0L) {
    stop("No simulation files found in: ", simulation_directory)
  }
  #sim_files <- sim_files[seq_len(min(5L, length(sim_files)))]

  data <- rbindlist(
    lapply(
      sim_files,
      function(f) {
        tryCatch({
          simdt <- readRDS(f)
          # Validate join keys exist in sim
          needed_join <- c("commodity_year","state_code","county_code","commodity_code","type_code",
                           "practice_code","unit_structure_code","insurance_plan_code","coverage_level_percent")
          miss <- setdiff(needed_join, names(simdt))
          if (length(miss)) stop("Missing in sim: ", paste(miss, collapse=", "))
          # data[simdt, on=...] keeps agent rows that match, adds sim columns
          out <- data[simdt, on = needed_join, nomatch = 0]
          out
        }, error = function(e) NULL)
      }), fill = TRUE);gc()

  # Stop if nothing joined / all files failed
  if (is.null(data) || nrow(data) == 0L) {
    stop("No simulation rows matched after join (or all simulation files failed to read).")
  }

  # Ensure disaggregate column exists if requested
  if (!is.null(disaggregate) && !(disaggregate %in% names(data))) {
    data[, (disaggregate) := "ALL"]
  }

  # Validate required numeric columns before use
  required_numeric <- c("revenue","indemnity","producer_premium","liability","total_premium","subsidy_amount")
  missing_numeric <- setdiff(required_numeric, names(data))
  if (length(missing_numeric)) {
    stop("Missing required numeric columns in joined data: ", paste(missing_numeric, collapse = ", "))
  }

  # Revenues (remove pmax(0, ...) if truncation is NOT intended)
  # Revenue_Inc = insured revenue: raw revenue + indemnity - producer premium, floored at zero
  data[, Revenue_Inc := pmax(0, revenue + indemnity - producer_premium)]
  # Revenue = uninsured revenue (or baseline), floored at zero
  data[, Revenue     := pmax(0, revenue)]

  # Expected stats
  # Means and SDs by group (uid)
  Expected <- data[, .(
    Revenue.mean     = mean(Revenue, na.rm=TRUE),
    Revenue.sd       = sd(Revenue, na.rm=TRUE),
    Revenue_Inc.mean = mean(Revenue_Inc, na.rm=TRUE),
    Revenue_Inc.sd   = sd(Revenue_Inc, na.rm=TRUE)
  ), by = uid];gc()

  # Replace non-finite mean/sd values with NA
  for (var in c("Revenue","Revenue_Inc")) {
    Expected[ , (paste0(var,".sd"))   := ifelse(is_bad(get(paste0(var,".sd"))),   NA_real_, get(paste0(var,".sd")))]
    Expected[ , (paste0(var,".mean")) := ifelse(is_bad(get(paste0(var,".mean"))), NA_real_, get(paste0(var,".mean")))]
  }

  # Residual-based measures
  # Merge back means to row-level, compute residuals and downside indicators
  LPM <- merge(data, Expected, by = uid, all.x = TRUE)

  for (var in c("Revenue","Revenue_Inc")) {
    # Residual from the group mean
    LPM[ , (paste0(var, ".res"))  := get(var) - get(paste0(var, ".mean"))]
    # Squared residuals only when below mean (loss side); else NA
    LPM[, (paste0(var, ".lres2")) := data.table::fifelse(get(paste0(var, ".res")) > 0, NA_real_, get(paste0(var, ".res"))^2)]
    # CDF indicator: 1 if below mean (loss), 0 otherwise; mean of this is P(loss)
    LPM[, (paste0(var, ".cdf"))   := data.table::fifelse(get(paste0(var, ".res")) < 0, 1.0, 0.0)]
  }

  # Collapse residual-based columns to group means
  LPM <- LPM[, lapply(.SD, function(x) mean(x, na.rm=TRUE)),
             by = uid,
             .SDcols = c(grep("\\.lres2$", names(LPM), value=TRUE),
                         grep("\\.cdf$",   names(LPM), value=TRUE))]

  # Add residual-based summaries to Expected
  Expected <- merge(Expected, LPM, by = uid, all.x = TRUE);rm(LPM);gc()

  # Derived stats
  # Variance, coefficient of variation, downside partial variance (lapv),
  # loss-ratio partial variance (lrpv = lapv / P(loss)), and their normalized versions
  for (var in c("Revenue","Revenue_Inc")) {
    Expected[, paste0(var, ".var")  := get(paste0(var, ".sd"))^2]
    Expected[, paste0(var, ".cv")   := safe_div(get(paste0(var, ".sd")), get(paste0(var, ".mean")))]
    Expected[, paste0(var, ".lapv") := get(paste0(var, ".lres2"))]
    Expected[, paste0(var, ".lrpv") := safe_div(get(paste0(var, ".lapv")), get(paste0(var, ".cdf")))]
    Expected[, paste0(var, ".nlapv"):= safe_div(get(paste0(var, ".lapv")), get(paste0(var, ".mean")))]
    Expected[, paste0(var, ".nlrpv"):= safe_div(get(paste0(var, ".lrpv")), get(paste0(var, ".mean")))]
  }

  # Relative measures
  # Ratios of insured vs. uninsured statistics (e.g., Relcv < 1 implies risk reduction)
  Expected[, `:=`(
    Relmean   = safe_div(Revenue_Inc.mean, Revenue.mean),
    Relsd     = safe_div(Revenue_Inc.sd,   Revenue.sd),
    Relcv     = safe_div(Revenue_Inc.cv,   Revenue.cv),
    Rellapv   = safe_div(Revenue_Inc.lapv, Revenue.lapv),
    Rellrpv   = safe_div(Revenue_Inc.lrpv, Revenue.lrpv),
    Relnlapv  = safe_div(Revenue_Inc.nlapv,Revenue.nlapv),
    Relnlrpv  = safe_div(Revenue_Inc.nlrpv,Revenue.nlrpv),
    Relvar    = safe_div(Revenue_Inc.var,  Revenue.var)
  )]

  # Drop base Revenue* stats, keep only relative fields (Rel*) before merging performance
  drop_cols <- c("Revenue.mean","Revenue.sd","Revenue.var","Revenue.cv",
                 "Revenue.lapv","Revenue.lrpv","Revenue.nlapv","Revenue.nlrpv",
                 "Revenue_Inc.mean","Revenue_Inc.sd","Revenue_Inc.var","Revenue_Inc.cv",
                 "Revenue_Inc.lapv","Revenue_Inc.lrpv","Revenue_Inc.nlapv","Revenue_Inc.nlrpv")
  keep <- setdiff(names(Expected), drop_cols)
  Expected <- Expected[, ..keep]

  # Insurance performance (compute with grouped ops)
  # Means of rates, group sums for lr_*; also keep mean revenues for reference
  data <- data[, .(
    liability        = mean(liability,        na.rm=TRUE),
    total_premium    = mean(total_premium,    na.rm=TRUE),
    subsidy_amount   = mean(subsidy_amount,   na.rm=TRUE),
    producer_premium = mean(producer_premium, na.rm=TRUE),
    indemnity        = mean(indemnity,        na.rm=TRUE),
    Simrate          = mean(safe_div(total_premium, liability), na.rm=TRUE),
    SimrateP         = mean(safe_div(total_premium - subsidy_amount, liability), na.rm=TRUE),
    Simsuby          = mean(safe_div(subsidy_amount, total_premium), na.rm=TRUE),
    Simlcr           = mean(safe_div(indemnity, liability), na.rm=TRUE),
    lr_indemnity     = sum(indemnity, na.rm=TRUE),       # sum by group
    lr_premium       = sum(total_premium, na.rm=TRUE),   # sum by group
    Revenue          = mean(Revenue, na.rm=TRUE),
    Revenue_Inc      = mean(Revenue_Inc, na.rm=TRUE)
  ), by = uid]

  # Final merge: relative stats + insurance performance
  Expected <- merge(Expected, data, by = uid, all = TRUE);gc()

  # Write output file; when task_id is a range, collapse as "min-max"
  if (!dir.exists(output_directory)) dir.create(output_directory, recursive = TRUE)
  task_tag <- if (length(task_id) == 1L) as.character(task_id) else paste(range(task_id), collapse = "-")
  save_path <- file.path(output_directory, paste0("expected_", year, "_", task_tag, ".rds"))
  saveRDS(Expected, save_path)

  # Clean up and return path invisibly (useful in pipelines)
  rm(Expected,data);gc()

  invisible(save_path)

}

#' Aggregate and winsorize expected outcomes (year-level)
#'
#' Loads all per-task expected outcome files for a given year, aggregates them,
#' winsorizes key relative metrics within groups (5th-95th percentiles), and
#' saves a single cleaned file.
#'
#' @param year Integer. Year to aggregate.
#' @param expected_directory Character or NULL. Directory containing per-task
#'   `expected_*.rds` files for the year. If NULL, uses
#'   `file.path(study_environment$wd$dir_expected, year)`.
#' @param output_directory Character or NULL. Directory to write the aggregated
#'   file. If NULL, uses `study_environment$wd$dir_expected`.
#' @param study_environment List. Must provide `$wd$dir_expected` (and is used to
#'   resolve defaults when directories are NULL).
#' @param agent_identifiers Character vector. Grouping keys used for by-group
#'   winsorization (default: `c("commodity_year","state_code","county_code","type_code")`).
#' @param disaggregate Character or NULL. Optional extra grouping column (e.g.,
#'   `"combination"`). If provided but missing, it is created as `"ALL"`.
#'
#' @details
#' Reads all `.rds` files under `expected_directory`, binds them, computes 5th and
#' 95th percentiles for `Relmean`, `Relsd`, `Relcv`, `Rellapv`, `Rellrpv`,
#' `Relnlapv`, `Relnlrpv`, `Relvar` within each group, caps values to that range,
#' and writes `expected_<year>.rds` to `output_directory`.
#'
#' @return Invisibly returns the path to the saved file.
#'
#' @import data.table
#' @export
aggregate_expected_outcomes <- function(
    year,
    expected_directory = NULL,
    output_directory = NULL,
    study_environment,
    agent_identifiers = c("commodity_year","state_code","county_code","type_code"),
    disaggregate=NULL){

  # base grouping keys
  uid <- agent_identifiers
  if (!is.null(disaggregate)) uid <- c(uid, disaggregate)

  # Resolve directories from study_environment if not provided
  if (is.null(expected_directory)) {
    expected_directory <- file.path(study_environment$wd$dir_expected, year)
  }
  if (is.null(output_directory)) {
    output_directory <- study_environment$wd$dir_expected
  }

  # Collect files
  files <- list.files(path = expected_directory, recursive = TRUE, full.names = TRUE)
  if (length(files) == 0L) {
    stop("No expected outcome files found in: ", expected_directory)
  }

  # Read & stack
  Expected <- data.table::rbindlist(
    lapply(files, function(expected) {
      tryCatch(readRDS(expected), error = function(e) NULL)
    }),
    fill = TRUE
  )

  if (is.null(Expected) || nrow(Expected) == 0L) {
    stop("No rows loaded from expected files in: ", expected_directory)
  }

  data.table::setDT(Expected)

  # Ensure disaggregate column exists if requested
  if (!is.null(disaggregate) && !(disaggregate %in% names(Expected))) {
    Expected[, (disaggregate) := "ALL"]
  }

  # Metrics to winsorize
  rel_cols <- c("Relmean","Relsd","Relcv","Rellapv","Rellrpv","Relnlapv","Relnlrpv","Relvar")

  # Assert metrics exist
  missing_rel <- setdiff(rel_cols, names(Expected))
  if (length(missing_rel)) {
    stop("Missing required relative metric columns: ", paste(missing_rel, collapse = ", "))
  }

  # Helper for safe quantile per group/column
  qfun <- function(x, p) {
    vals <- unique(x[is.finite(x)])
    if (length(vals)) stats::quantile(vals, probs = p, names = FALSE, na.rm = TRUE) else NA_real_
  }

  # Upper limits (95th) by group
  upper_limits <- Expected[, lapply(.SD, qfun, p = 0.95),by = uid, .SDcols = rel_cols]
  names(upper_limits) <- c(uid, paste0("upper_", rel_cols))

  # Merge
  Expected <- merge(Expected, upper_limits, by = uid, all.x = TRUE)

  # Lower limits (5th) by group
  lower_limits <- Expected[, lapply(.SD, qfun, p = 0.05),by = uid, .SDcols = rel_cols]
  names(lower_limits) <- c(uid, paste0("lower_", rel_cols))

  # Merge
  Expected <- merge(Expected, lower_limits, by = uid, all.x = TRUE)

  # Winsorize: cap to [lower, upper] using pmin/pmax with na.rm=TRUE
  for(nm in rel_cols) {
    ucol <- paste0("upper_", nm)
    lcol <- paste0("lower_", nm)
    # upper cap first
    Expected[, (nm) := pmin(get(nm), get(ucol), na.rm = TRUE)]
    # lower cap
    Expected[, (nm) := pmax(get(nm), get(lcol), na.rm = TRUE)]
  }

  # Drop helper columns
  drop_cols <- grep("^(upper|lower)_", names(Expected), value = TRUE)
  if (length(drop_cols)) Expected[, (drop_cols) := NULL]

  # Save
  if (!dir.exists(output_directory)) dir.create(output_directory, recursive = TRUE)
  save_path <- file.path(output_directory, paste0("expected_", year, ".rds"))
  saveRDS(Expected, save_path)

  rm(Expected); gc()
  invisible(save_path)
}

