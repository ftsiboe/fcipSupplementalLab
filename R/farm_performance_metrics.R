#' Farm performance metrics by scenario and disaggregate
#'
#' @description
#' Load `expected_<year>.rds`, derive outcome variables, compute deltas vs. baselines,
#' trim extremes using quantile limits, aggregate (weighted mean/median) by requested
#' disaggregates, and save a summarized `.rds`. Returns the saved path invisibly.
#'
#' @details
#' Steps:
#' 1) Filter rows to `combination %in% {"Basic+CURRENT", combo, "Basic+SCO8665"}`.
#' 2) Create derived metrics: `rrs1/2/3`, `its`, flags `Irrs*`/`Iits`, `sner*`,
#'    percent/level transforms (`rrp*`, `itp`), and scale `Sim*` by 100.
#' 3) Reshape to long on `outcome_list`, drop non-finite values, average within
#'    identifiers (`agent_identifiers`, and `weight_variable` if provided), scenario, variable.
#' 4) Join baselines: if `combo != "Basic+CURRENT"`, add `"Basic+CURRENT"` as `base00`;
#'    if `combo = {"Basic+SCO8665","Basic+CURRENT"}`, add `"Basic+SCO8665"` as `base01`.
#'    Compute `chglvl00/01` and `chgpct00/01` (guard divide-by-zero).
#' 5) Build labels `PLAN`, `RPYP`, `COV`, `STRUCT`.
#' 6) Compute trimming limits per (`variable`, `combination`, `state_code`, `IRR`, `commodity_code`)
#'    using `distributional_limits` (default `c(0.05,0.95)`), require n greater or equal to 20, and cap to `*T` columns.
#' 7) For each of `c("FCIP", disaggregates)`, compute weighted mean and weighted median of
#'    raw and trimmed metrics; stack results and write output.
#'
#' @note
#' Baseline joins use `nomatch = 0` by design, so rows missing in the baseline are dropped
#' before delta computation. Change to `nomatch = NA` if you prefer to retain such rows with `NA` deltas.
#'
#' @section Required columns:
#' All `agent_identifiers`, plus:
#' `combination`, `state_code`, `county_code`, `commodity_code`, `type_code`, `practice_code`,
#' `IRR`, `Relcv`, `Relnlrpv`, `Relnlapv`, `Relmean`, `Simrate`, `SimrateP`, `Simsuby`, `Simlcr`,
#' `coverage_level_percent`, `unit_structure_code`, `insurance_plan_code`.
#' If `weight_variable` is not `NULL`, that column must exist and be numeric.
#'
#' @param year Policy year used to locate `expected_<year>.rds`.
#' @param agent_identifiers Character vector of ID columns for grouping prior to long-pivot and averaging.
#' @param outcome_list Character vector of outcome columns to reshape and aggregate.
#' @param combo Target scenario (e.g., `"Basic+CURRENT"`, `"Basic+SCO8665"`, or another).
#' @param weight_variable `NULL` for equal weights (=1) or a character name of a numeric weight column.
#' @param expected_directory Directory containing `expected_<year>.rds`.
#' @param draw Optional draw identifier used for filtering and filename tag.
#' @param draw_list_file_path Optional path to an RDS (named list) with the draw table; required if `draw` is not `NULL`.
#' @param disaggregates Optional character vector of additional disaggregate columns (alongside `"FCIP"`).
#' @param output_file_path Output file path
#' @param distributional_limits Numeric length-2 vector of lower/upper probabilities (e.g., `c(0.05, 0.95)`); must satisfy `0 < p1 < p2 < 1`.
#'
#' @returns Invisibly returns the character path of the saved `.rds`.
#'
#' @seealso data.table::data.table, data.table::melt, matrixStats::weightedMedian
#' @import data.table
#' @importFrom data.table melt :=
#' @importFrom matrixStats weightedMedian
#' @importFrom stats weighted.mean quantile na.omit
#'
#' @export
farm_performance_metrics <- function(
    year,
    agent_identifiers = c("commodity_year","state_code","county_code","commodity_code","type_code","practice_code",
                          "unit_structure_code","insurance_plan_code","coverage_level_percent"),
    outcome_list = c("its","Iits","rrs1","rrs2","rrs3","Irrs1","Irrs2","Irrs3","sner1","sner2","sner3",
                     "Simrate","SimrateP","Simsuby","Simlcr","rrp1","rrp2","rrp3","itp"),
    combo,
    weight_variable = NULL,
    expected_directory = NULL,
    draw = NULL,
    draw_list_file_path = NULL,
    disaggregates = NULL,
    output_file_path = NULL,
    distributional_limits = c(0.05, 0.95)) {

  # --- basic validation
  if (!is.null(draw) & is.null(draw_list_file_path)) {
    stop("Provide path to drawing list when draw is not NULL")
  }
  if (!is.numeric(distributional_limits) || length(distributional_limits) != 2 ||
      anyNA(distributional_limits) || distributional_limits[1] <= 0 ||
      distributional_limits[2] >= 1 || distributional_limits[1] >= distributional_limits[2]) {
    stop("`distributional_limits` must be a numeric length-2 vector like c(0.05, 0.95) with 0<p1<p2<1.")
  }

  path_in <- file.path(expected_directory, paste0("expected_", year, ".rds"))
  if (is.null(expected_directory) || !file.exists(path_in)) {
    stop("Expected file not found: ", path_in)
  }

  # --- load & preflight
  data <- readRDS(path_in)
  data.table::setDT(data)
  data <- data[combination %in% unique(c("Basic+CURRENT", combo, "Basic+SCO8665"))]

  # data <- data[commodity_code %in% 41]

  req_cols <- c(agent_identifiers, "combination", "Relcv","Relnlrpv","Relnlapv","Relmean",
                "Simrate","SimrateP","Simsuby","Simlcr","coverage_level_percent",
                "unit_structure_code","insurance_plan_code","state_code","commodity_code")
  missing_req <- setdiff(req_cols, names(data))
  if (length(missing_req)) stop("Missing required columns: ", paste(missing_req, collapse = ", "))

  # --- weights & working column sets
  by_id     <- c(agent_identifiers, "combination", "variable")
  keep_cols <- c(agent_identifiers, "combination", outcome_list)
  if (is.null(weight_variable)) {
    data[, WEIGHTS := 1]
  } else {
    data[, WEIGHTS := get(weight_variable)]
    by_id     <- c(agent_identifiers, "WEIGHTS", "combination", "variable")
    keep_cols <- c(agent_identifiers, "WEIGHTS", "combination", outcome_list)
  }

  # --- derived outcomes
  data[, `:=`(
    rrs1 = Relcv,
    rrs2 = Relnlrpv,
    rrs3 = Relnlapv,
    its  = Relmean,
    Irrs1 = Relcv     < 1,
    Irrs2 = Relnlrpv  < 1,
    Irrs3 = Relnlapv  < 1,
    Iits  = Relmean   > 1
  )]

  data[, `:=`(
    sner1 = fifelse(Irrs1 & Iits & its != 1, -((rrs1 - 1) / (its - 1)), 0.0),
    sner2 = fifelse(Irrs2 & Iits & its != 1, -((rrs2 - 1) / (its - 1)), 0.0),
    sner3 = fifelse(Irrs3 & Iits & its != 1, -((rrs3 - 1) / (its - 1)), 0.0),

    rrp1 = -100 * (rrs1 - 1),
    rrp2 = -100 * (rrs2 - 1),
    rrp3 = -100 * (rrs3 - 1),
    itp  =  100 * (its  - 1),

    Simrate  = Simrate  * 100,
    Simsuby  = Simsuby  * 100,
    SimrateP = SimrateP * 100,
    Simlcr   = Simlcr   * 100
  )]

  # Coerce all outcomes to numeric (simple & robust)
  data[, (outcome_list) := lapply(.SD, function(x) {
    as.numeric(as.character(x))
  }), .SDcols = outcome_list]

  # select, long-form, and average
  data <- data[, ..keep_cols]
  data <- melt(data, id.vars = setdiff(keep_cols, outcome_list),
               measure.vars = outcome_list,
               variable.name = "variable", value.name = "value")
  data <- data[is.finite(value) & !is.na(value)]
  data <- data[, .(value = mean(value, na.rm = TRUE)), by = by_id]

  # --- baselines (keeps your `nomatch=0` behavior)
  if (!(combo %in% "Basic+CURRENT")) {
    base <- data[combination == "Basic+CURRENT", .(value, variable, combo_key = do.call(paste, c(.SD, sep = "\r"))),
                 .SDcols = agent_identifiers]
    setnames(base, "value", "base00")
    # join by intersection of columns; we use a robust combo_key to avoid constructing long 'on' clauses repeatedly
    data[, combo_key := do.call(paste, c(.SD, sep = "\r")), .SDcols = agent_identifiers]
    data <- base[data[combination != "Basic+CURRENT"], on = .(combo_key, variable), nomatch = 0][
      , !"combo_key"]
    # restore original order of columns
    setcolorder(data, c(agent_identifiers, "combination", "variable", "value", "base00"))
    rm(base);gc()
  }

  if (!(combo %in% c("Basic+SCO8665","Basic+CURRENT"))) {
    base <- data[combination == "Basic+SCO8665", .(value, variable, combo_key = do.call(paste, c(.SD, sep = "\r"))),
                 .SDcols = agent_identifiers]
    setnames(base, "value", "base01")
    if (!"combo_key" %in% names(data)) {
      data[, combo_key := do.call(paste, c(.SD, sep = "\r")), .SDcols = agent_identifiers]
    }
    data <- base[data[combination != "Basic+SCO8665"], on = .(combo_key, variable), nomatch = 0][
      , !"combo_key"]
    setcolorder(data, c(agent_identifiers, "combination", "variable", setdiff(names(data), c(agent_identifiers, "combination", "variable"))))
    rm(base);gc()
  }

  # --- keep target scenario only
  data <- data[combination %in% combo]

  # --- labels (PLAN / RPYP / COV / STRUCT)
  data[, `:=`(
    FCIP = 1,
    PLAN = fcase(
      insurance_plan_code %in% c(1, 90), "Yield",
      insurance_plan_code %in% c(2, 3),  "Revenue",
      default = NA_character_
    ),
    RPYP = fcase(
      insurance_plan_code %in% c(1, 90), "YP",
      insurance_plan_code == 2,          "RP",
      insurance_plan_code == 3,          "RPHPE",
      default = NA_character_
    ),
    COV = fcase(
      round(coverage_level_percent * 100) %in% c(50, 55), "50-55%",
      round(coverage_level_percent * 100) %in% c(60, 65), "60-65%",
      round(coverage_level_percent * 100) %in% c(70, 75), "70-75%",
      round(coverage_level_percent * 100) %in% c(80, 85), "80-85%",
      default = NA_character_
    ),
    STRUCT = fcase(
      unit_structure_code == "BU",                        "BU",
      unit_structure_code %in% c("EU","WU","EP","EC"),   "EU",
      unit_structure_code %in% c("OU","UA","UD"),        "OU",
      default = "OU"
    )
  )]

  # --- deltas
  if ("base00" %in% names(data)) {
    data[, chglvl00 := value - base00]
    data[, chgpct00 := fifelse(is.na(base00) | base00 == 0, NA_real_, ((value - base00)/abs(base00))*100)]
  }
  if ("base01" %in% names(data)) {
    data[, chglvl01 := value - base01]
    data[, chgpct01 := fifelse(is.na(base01) | base01 == 0, NA_real_, ((value - base01)/abs(base01))*100)]
  }

  # --- Optional filter to drawing list
  if (!is.null(draw) && !is.null(draw_list_file_path)) {
    if (!file.exists(draw_list_file_path)) {
      stop("draw_list_file_path not found: ", draw_list_file_path)
    }
    dl <- readRDS(draw_list_file_path)
    if (!is.list(dl) || is.null(dl[[draw]])) {
      stop("Draw '", draw, "' not found in draw_list_file_path.")
    }

    draw_tbl <- as.data.table(dl[[draw]])

    # ensure the expected join keys are present
    join_keys <- c("state_code","county_code","commodity_code","type_code","practice_code")
    missing_keys <- setdiff(join_keys, names(draw_tbl))
    if (length(missing_keys)) {
      stop("Draw table is missing required keys: ", paste(missing_keys, collapse = ", "))
    }

    # fast inner join: keep only rows present in the draw list
    setkeyv(draw_tbl, join_keys)
    data <- data[draw_tbl, on = join_keys, nomatch = 0,allow.cartesian=TRUE];rm(draw_tbl)
  }

  # ensure change columns exist for all paths
  missing <- setdiff(c("chgpct01","chglvl01","chgpct00","chglvl00"), names(data))
  if (length(missing) > 0) data[, (missing) := lapply(missing, function(x) as.numeric(NA))]

  # --- limits & trimming
  p_lo <- distributional_limits[1]; p_hi <- distributional_limits[2]
  # compute limits by group; set key for faster join
  lim_by <- c("variable","combination","state_code","IRR","commodity_code")
  setkeyv(data, lim_by)

  data_limits <- data[, .(
    ll_nn       = .N,
    ll_value    = quantile(value,    probs = p_lo, na.rm = TRUE),
    ul_value    = quantile(value,    probs = p_hi, na.rm = TRUE),
    ll_chglvl00 = quantile(chglvl00, probs = p_lo, na.rm = TRUE),
    ul_chglvl00 = quantile(chglvl00, probs = p_hi, na.rm = TRUE),
    ll_chgpct00 = quantile(chgpct00, probs = p_lo, na.rm = TRUE),
    ul_chgpct00 = quantile(chgpct00, probs = p_hi, na.rm = TRUE),
    ll_chglvl01 = quantile(chglvl01, probs = p_lo, na.rm = TRUE),
    ul_chglvl01 = quantile(chglvl01, probs = p_hi, na.rm = TRUE),
    ll_chgpct01 = quantile(chgpct01, probs = p_lo, na.rm = TRUE),
    ul_chgpct01 = quantile(chgpct01, probs = p_hi, na.rm = TRUE)
  ), by = lim_by][ll_nn >= 20]

  setkeyv(data_limits, lim_by)
  data <- data[data_limits, nomatch = 0];rm(data_limits)

  # vectorized trims with pmin/pmax to reduce passes
  data[, chgpct00T := pmin(pmax(chgpct00, -100), ul_chgpct00)]
  data[, chglvl00T := pmin(pmax(chglvl00, ll_chglvl00), ul_chglvl00)]
  data[, chgpct01T := pmin(pmax(chgpct01, -100), ul_chgpct01)]
  data[, chglvl01T := pmin(pmax(chglvl01, ll_chglvl01), ul_chglvl01)]
  data[, valueT    := pmin(pmax(value,    ll_value),    ul_value)]

  # --- aggregate by disaggregates
  agg_cols <- c("value","valueT","chglvl00","chgpct00","chglvl01","chgpct01","chgpct00T","chglvl00T","chgpct01T","chglvl01T")

  if (!is.null(disaggregates)) {
    bad_disag <- setdiff(disaggregates, names(data))
    if (length(bad_disag)) warning("These disaggregates are absent and will be skipped: ",
                                   paste(bad_disag, collapse = ", "))
  }

  # build list of tags to iterate (skip NULL gracefully)
  disags_to_run <- unique(na.omit(c("FCIP", disaggregates)))

  data <- lapply(disags_to_run, function(disag_tag) {
    tryCatch({
      df <- copy(data)
      df[, level := get(disag_tag)]

      data_avg <- df[, lapply(.SD, function(x) stats::weighted.mean(x, w = WEIGHTS, na.rm = TRUE)),
                     by = c("variable","combination","level"), .SDcols = agg_cols]
      data_med <- df[, lapply(.SD, function(x) matrixStats::weightedMedian(x, w = WEIGHTS, na.rm = TRUE)),
                     by = c("variable","combination","level"), .SDcols = agg_cols]

      # prefix columns without constructing each name manually
      non_id <- setdiff(names(data_avg), c("variable","combination","level"))
      setnames(data_avg, non_id, paste0("avg_", non_id))
      setnames(data_med, non_id, paste0("med_", non_id))

      data_avg <- melt(data_avg, id.vars = c("variable","combination","level"),
                       variable.name = "aggregation", value.name = "value")
      data_med <- melt(data_med, id.vars = c("variable","combination","level"),
                       variable.name = "aggregation", value.name = "value")

      df <- rbindlist(list(data_avg, data_med), use.names = TRUE)
      df[, disag := disag_tag]
      df <- df[is.finite(value) & !is.na(value),
               .(variable, combination, disag, level, aggregation, value)]
      return(df)
    }, error = function(e) {
      NULL
    })
  })

  data <- rbindlist(data, use.names = TRUE, fill = TRUE)

  # --- add tags & save
  if (!is.null(draw)) data[, draw_id := draw]
  data[, commodity_year := year]

  if (!dir.exists(dirname(output_file_path))) dir.create(dirname(output_file_path), recursive = TRUE)
  saveRDS(data, output_file_path)

  invisible(output_file_path)
}



