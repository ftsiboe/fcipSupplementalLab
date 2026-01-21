#' Build agent simulation panel
#'
#' Read cleaned agent-level simulation data for a crop year, unnest per-draw
#' outcomes, filter to the requested draw(s), compute county-level expected
#' yields, and add per-row revenue.
#'
#' @details
#' The function:
#' \enumerate{
#'   \item Loads \code{cleaned_agents_data_<year>.rds} from \code{agents_directory}.
#'   \item Unnests draw pools: number, farm yield/price, and county yield/price.
#'   \item Filters to \code{sim} (matching \code{rma_draw_number}).
#'   \item Renames simulated fields to canonical names and floors negative
#'         county yields at zero.
#'   \item Computes a planted-acre-weighted \code{expected_county_yield}.
#'   \item Computes row-level \code{revenue = actual_farm_yield * actual_price * planted_acres}.
#' }
#'
#' @param year Integer. Crop year.
#' @param sim Integer vector. Draw number(s) to keep.
#' @param agents_directory Character. Directory containing cleaned agent data.
#'   Default: \code{"data/cleaned_agents_data"}.
#'
#' @return A \link[data.table]{data.table} containing all original columns plus:
#'   \itemize{
#'     \item \code{expected_county_yield}
#'     \item \code{final_county_yield}
#'     \item \code{harvest_price}
#'     \item \code{revenue}
#'   }
#'
#' @import data.table
#' @importFrom stats weighted.mean
#' @export
build_agent_simulation_data <- function(
    year,
    sim,
    agents_directory = "data/cleaned_agents_data"
){

  # Load cleaned agents data
  agents_file <- file.path(agents_directory, sprintf("cleaned_agents_data_%s.rds", year))
  dt <- readRDS(agents_file)
  data.table::setDT(dt)
  # dt <- dt[commodity_code %in% 18]

  # Unnest list-columns
  dt <- dt |> tidyr::unnest(cols = c(rma_draw_number,rma_draw_yield_farm,rma_draw_price_farm,rma_draw_yield_pool,rma_draw_price_pool))
  dt <- as.data.table(dt)
  dt <- dt[rma_draw_number %in% sim];gc()

  # Rename/clean simulated outcomes
  dt[rma_draw_yield_pool < 0, rma_draw_yield_pool := 0]

  data.table::setnames(dt, old = c("rma_draw_yield_farm", "rma_draw_price_farm","rma_draw_yield_pool","rma_draw_price_pool"),
                       new = c("actual_farm_yield", "actual_price","final_county_yield","harvest_price"))

  dt[,expected_county_yield := as.numeric(weighted.mean(approved_yield, planted_acres, na.rm = TRUE)),
     by = c("commodity_year","state_code","county_code","commodity_code","type_code","practice_code")]

  # Revenue
  dt[, revenue := actual_farm_yield * actual_price * planted_acres]

  # Return result
  return(dt[])
}


#' Compute base-policy outcomes
#'
#' Vectorized \pkg{data.table} implementation of base-policy guarantees,
#' acres/liability, premium pieces (total/subsidy/producer), and indemnity, plus
#' a tidy column subset for downstream joins.
#'
#' @details
#' Requires a set of core inputs (e.g., yields, prices, coverages, acres) and
#' returns the standard monetary outputs for each policy row. Price risk is
#' handled via a \code{new_insurance_guarantee} that depends on plan code.
#'
#' @param cleaned_agents_data A \code{data.frame} or \code{data.table} with the
#'   required columns (see error message if any are missing).
#'
#' @return A \link[data.table]{data.table} with key fields and outputs:
#'   \code{insured_acres}, \code{liability}, \code{total_premium},
#'   \code{subsidy_amount}, \code{producer_premium}, \code{indemnity},
#'   \code{revenue}, and supporting fields such as \code{harvest_price},
#'   \code{expected_county_yield}, \code{final_county_yield},
#'   \code{new_insurance_guarantee}, \code{projected_price}.
#'
#' @import data.table
#' @export
compute_base_policy_outcomes <- function(cleaned_agents_data) {
  # required inputs
  req <- c(
    "approved_yield","coverage_level_percent","price_election",
    "planted_acres","insured_share","observed_premium_rate","observed_subsidy_percent",
    "damage_area_rate","harvest_price","insurance_plan_code",
    "actual_farm_yield","actual_price",
    "commodity_year","state_code","county_code","commodity_code",
    "type_code","practice_code","unit_structure_code","coverage_type_code",
    "coverage_level_percent","sco","eco90","eco95","expected_county_yield",
    "final_county_yield","projected_price"
  )
  miss <- setdiff(req, names(cleaned_agents_data))
  if (length(miss)) stop("Missing required columns: ", paste(miss, collapse = ", "))

  dt <- data.table::as.data.table(data.table::copy(cleaned_agents_data))

  # calculations

  # 1) primitives used later
  dt[, `:=`(
    guaranteed_yield      = round(approved_yield * coverage_level_percent, 1),
    price_election_amount = projected_price * price_election
  )]

  # 2) insurance guarantee
  dt[, insurance_guarantee := round(guaranteed_yield * price_election_amount, 2)]

  # 3) acres & liability
  dt[, insured_acres := planted_acres * insured_share]
  dt[, liability     := round(insurance_guarantee * insured_acres, 0)]

  # 4) premium pieces
  dt[, total_premium  := round(observed_premium_rate * liability)]
  dt[, subsidy_amount := round(total_premium * round(observed_subsidy_percent, 2))]
  dt[, producer_premium := round(total_premium - subsidy_amount)]

  # 5) price risk & new guarantee
  dt[, determined_acreage := planted_acres * damage_area_rate]
  dt[, price_risk := pmax(pmin(price_election_amount * 2, harvest_price),
                          price_election_amount)]
  dt[, new_insurance_guarantee := data.table::fifelse(
    insurance_plan_code %in% 2,
    price_risk * guaranteed_yield,
    insurance_guarantee
  )]

  # 6) revenue to count & indemnity
  dt[, revenue_to_count := actual_farm_yield * data.table::fifelse(
    insurance_plan_code %in% c(1, 90),
    price_election_amount, harvest_price
  )]
  dt[, indemnity := data.table::fifelse(
    revenue_to_count < new_insurance_guarantee,
    new_insurance_guarantee - revenue_to_count, 0
  ) * determined_acreage * insured_share]

  # 7) revenue
  dt[, revenue := actual_farm_yield * actual_price * planted_acres]

  # final column order/subset
  keep <- c(
    "commodity_year","state_code","county_code","commodity_code","type_code","practice_code",
    "unit_structure_code","insurance_plan_code","coverage_type_code","coverage_level_percent",
    "sco","eco90","eco95",
    "actual_farm_yield","actual_price","expected_county_yield","final_county_yield","harvest_price",
    "projected_price","new_insurance_guarantee","liability","insured_acres",
    "total_premium","subsidy_amount","producer_premium","indemnity","revenue"
  )
  # keep only those that exist (in case some optional inputs are absent)
  keep <- intersect(keep, names(dt))
  out <- dt[, ..keep]

  return(out[])
}


#' Build study scenarios (SCO/ECO offerings and mixes)
#'
#' Define the endorsement offerings (plan family - trigger - subsidy - label)
#' and the full-participation SCO/ECO mixes to evaluate for a given year.
#'
#' @details
#' For years \code{>= 2021}, ECO 90/44 and 95/44 variants are added and the
#' participation set is expanded accordingly. Offerings create \code{sup}
#' labels such as \code{"SCO8665"}, \code{"SCO9080"}, \code{"ECO9044"},
#' \code{"ECO9544"}.
#'
#' @param year Integer. Crop year used to determine available ECO variants.
#'
#' @return A named list with:
#' \itemize{
#'   \item \code{offerings}: \link[data.table]{data.table} of
#'         \code{insurance_plan_code}, \code{Trigger}, \code{plan}, \code{Subsidy_factor}.
#'   \item \code{full_participation}: \link[data.table]{data.table} of
#'         SCO/ECO label combinations to test (columns \code{sco}, \code{eco}).
#' }
#'
#' @import data.table
#' @export
study_scenarios <- function(year){
  # base SCO/ECO offerings
  offerings <- data.table::rbindlist(list(
    data.table::data.table(insurance_plan_code = 31:33, Trigger = 0.86, plan = "sco8665", Subsidy_factor = 0.65),
    # OB3: raise SCO to 90% with 80% subsidy
    data.table::data.table(insurance_plan_code = 51:53, Trigger = 0.90, plan = "sco9080", Subsidy_factor = 0.80),
    data.table::data.table(insurance_plan_code = 87:89, Trigger = 0.90, plan = "eco9080", Subsidy_factor = 0.80),
    data.table::data.table(insurance_plan_code = 87:89, Trigger = 0.95, plan = "eco9580", Subsidy_factor = 0.80)
  ), use.names = TRUE)

  # SCO/ECO combinations to consider in full participation scenarios
  full_participation <- data.table::data.table(
    sco = c("SCO8665","SCO8680","SCO8865","SCO8880","SCO9065","SCO9080"),
    eco = ""
  )

  if (year >= 2021) {
    offerings <- data.table::rbindlist(list(
      offerings,
      data.table::data.table(insurance_plan_code = 87:89, Trigger = 0.90, plan = "eco9044", Subsidy_factor = 0.44),
      data.table::data.table(insurance_plan_code = 87:89, Trigger = 0.95, plan = "eco9544", Subsidy_factor = 0.44)
    ), use.names = TRUE)

    full_participation <- data.table::rbindlist(list(
      full_participation,
      data.table::data.table(sco = "", eco = c("ECO9044","ECO9544","ECO9080","ECO9580")),
      data.table::data.table(sco = c("SCO8665","SCO8680","SCO8865","SCO8880"), eco = "ECO9044"),
      data.table::data.table(sco = c("SCO8665","SCO8680","SCO8865","SCO8880"), eco = "ECO9080"),
      data.table::data.table(sco = c("SCO8665","SCO8680","SCO8865","SCO8880","SCO9065","SCO9080"), eco = "ECO9544"),
      data.table::data.table(sco = c("SCO8665","SCO8680","SCO8865","SCO8880","SCO9065","SCO9080"), eco = "ECO9580")
    ), use.names = TRUE, fill = TRUE)
  }

  list(
    offerings = offerings,
    full_participation = full_participation
  )
}


#' Compute supplemental policy factors (SCO/ECO)
#'
#' Compute shallow-loss protection, premiums, and indemnities for one SCO/ECO
#' endorsement offering, aligning plan families and joining ADM rating inputs.
#'
#' @details
#' Handles plan families via offsets (31-33, 41-43, 51-53, 87-89). For plans
#' 87-89 (ECO), the \code{coverage_level_percent} for ADM is matched to the
#' \code{trigger} (with a small tolerance), and the subsidy factor special-case
#' is applied for underlying plan code 1. Emits a standard \code{sup} label
#' like \code{"SCO8665"} or \code{"ECO9544"}.
#'
#' @param base_policy \link[data.table]{data.table}. Base-policy rows (keys,
#'   yields, prices, liability, etc.).
#' @param adm \link[data.table]{data.table}. Rating inputs with
#'   \code{base_rate} and join keys.
#' @param plan Integer. Plan code in the offering (e.g., 31-33, 51-53, 87-89).
#' @param subsidy Numeric. Subsidy factor (e.g., \code{0.65}, \code{0.80}, \code{0.44}).
#' @param trigger Numeric. Coverage trigger level (e.g., \code{0.86}, \code{0.90}, \code{0.95}).
#'
#' @return A \link[data.table]{data.table} with columns:
#'   \code{commodity_year}, \code{state_code}, \code{county_code},
#'   \code{commodity_code}, \code{type_code}, \code{practice_code},
#'   \code{unit_structure_code}, \code{insurance_plan_code},
#'   \code{coverage_level_percent}, \code{liability}, \code{total_premium},
#'   \code{subsidy_amount}, \code{producer_premium}, \code{indemnity}, \code{sup}.
#'
#' @import data.table
#' @export
compute_supplemental_factors <- function(base_policy, adm, plan, subsidy, trigger) {

  offset <- fcase(
    plan %in% 31:33, 30L,
    plan %in% 41:43, 40L,
    plan %in% 51:53, 50L,
    plan %in% 87:89, 86L,
    default = NA_integer_
  )

  if (!is.na(offset)) {
    if (offset == 86L) {
      # special case for plans 87-89
      base_policy[, ENDOS_Subsidy_factor := fifelse(insurance_plan_code %in% 1L, 0.51, subsidy)]
      base_policy[, END_coverage_level_percent := 0.86]
      adm <- adm[abs(coverage_level_percent - trigger) < 1e-8, !"coverage_level_percent"]
    } else {
      # common case for 31-33, 41-43, 51-53
      base_policy[, ENDOS_Subsidy_factor := fifelse(insurance_plan_code %in% 1:3, subsidy, NA_real_)]
      base_policy[, END_coverage_level_percent := coverage_level_percent]
    }

    adm[, insurance_plan_code := insurance_plan_code - offset]
    base_policy <- base_policy[insurance_plan_code %in% (plan - offset)]
  }

  # Shallow loss protection
  base_policy[, expected_county_value := expected_county_yield * projected_price]
  base_policy[, final_county_value    := final_county_yield  * harvest_price]
  base_policy[, Coverage_Range := trigger - END_coverage_level_percent]
  base_policy[, expected_county_value_implied := liability / coverage_level_percent]
  base_policy[, ENDOS_protection := expected_county_value_implied * Coverage_Range]

  # Shallow loss premium
  base_policy <- merge(
    base_policy,
    adm,
    by = names(adm)[names(adm) %in% names(base_policy)],
    all = TRUE
  )
  base_policy <- base_policy[is.finite(insured_acres) & insured_acres > 0]

  common_col <- c("state_code","county_code","commodity_code","insurance_plan_code","coverage_level_percent")
  base_policy <- base_policy[
    base_rate %in% c(NA, Inf, Inf, NaN, 0),
    base_rate := mean(base_rate, na.rm = TRUE),
    by = c(names(base_policy)[names(base_policy) %in% c(common_col, "practice_code")])
  ]
  base_policy <- base_policy[
    base_rate %in% c(NA, Inf, Inf, NaN, 0),
    base_rate := mean(base_rate, na.rm = TRUE),
    by = c(names(base_policy)[names(base_policy) %in% c(common_col, "type_code")])
  ]
  base_policy <- base_policy[
    base_rate %in% c(NA, Inf, Inf, NaN, 0),
    base_rate := mean(base_rate, na.rm = TRUE),
    by = c(names(base_policy)[names(base_policy) %in% c(common_col)])
  ]
  base_policy <- base_policy[is.finite(base_rate) & base_rate > 0]

  base_policy[, ENDOS_Total_Premium    := ENDOS_protection * base_rate]
  base_policy[, ENDOS_Subsidy_amount   := ENDOS_Subsidy_factor * ENDOS_Total_Premium]
  base_policy[, ENDOS_Producer_Premium := ENDOS_Total_Premium - ENDOS_Subsidy_amount]

  # Shallow loss indemnity
  base_policy[, New_Expected_Crop_Value := new_insurance_guarantee / coverage_level_percent]
  base_policy[, New_ENDOS_protection    := New_Expected_Crop_Value * Coverage_Range * insured_acres]
  base_policy[, ENDOS_Reduction_rate := fcase(
    insurance_plan_code %in% 1L, trigger - (final_county_yield / expected_county_yield),
    insurance_plan_code %in% 3L, trigger - (final_county_value / expected_county_value),
    insurance_plan_code %in% 2L, trigger - (final_county_value / (expected_county_yield * harvest_price)),
    default = NA_real_
  )]
  base_policy[, ENDOS_Reduction_rate := pmax(ENDOS_Reduction_rate, 0)]
  base_policy[, ENDOS_Payment_Factor := pmin(ENDOS_Reduction_rate / Coverage_Range, 1)]
  base_policy[, ENDOS_Indemnity := ENDOS_Payment_Factor * New_ENDOS_protection]

  # map ENDOS* to standard output fields
  base_policy[, `:=`(
    liability        = ENDOS_protection,
    total_premium    = ENDOS_Total_Premium,
    subsidy_amount   = ENDOS_Subsidy_amount,
    producer_premium = ENDOS_Producer_Premium,
    indemnity        = ENDOS_Indemnity
  )]

  # keep only required columns (order preserved)
  base_policy <- base_policy[, .(
    commodity_year, state_code, county_code, commodity_code, type_code, practice_code,
    unit_structure_code, insurance_plan_code, coverage_level_percent,
    liability, total_premium, subsidy_amount, producer_premium, indemnity
  )]

  # sup from supplist 'plan' field
  sup_prefix <- if (plan %in% 87:89) "ECO" else "SCO"
  sup_trig   <- sprintf("%02d", as.integer(round(trigger * 100)))  # 0.86 -> "86"
  sup_sub    <- sprintf("%02d", as.integer(round(subsidy * 100)))  # 0.65 -> "65"
  base_policy[, sup := paste0(sup_prefix, sup_trig, sup_sub)]

  return(base_policy)
}


#' Aggregate supplemental results for the current environment
#'
#' Scale selected SCO/ECO factors by base-policy weights (\code{sco}, \code{eco90},
#' \code{eco95}), aggregate by policy keys, append base outcomes, and label the
#' rollup as \code{"Basic+CURRENT"}.
#'
#' @param base_policy_data \link[data.table]{data.table}. Base-policy outcomes
#'   (contains keys, weights, and monetary fields).
#' @param supplemental_factors \link[data.table]{data.table}. Supplemental outcomes
#'   from \code{\link{compute_supplemental_factors}} including \code{sup}.
#'
#' @return A \link[data.table]{data.table} aggregated by policy keys with:
#'   \code{revenue}, \code{liability}, \code{total_premium}, \code{subsidy_amount},
#'   \code{producer_premium}, \code{indemnity}, and \code{combination}.
#'
#' @import data.table
#' @export
compute_supplemental_current <- function(base_policy_data,supplemental_factors){

  supplemental_current <- copy(base_policy_data)[
    , c("commodity_year","state_code","county_code","commodity_code","type_code","practice_code",
        "unit_structure_code","insurance_plan_code","coverage_level_percent","sco", "eco90", "eco95"), with = FALSE]

  supplemental_current <- supplemental_current[
    is.finite(
      supplemental_current[, rowSums(.SD, na.rm = TRUE),
                           .SDcols = c("sco", "eco90", "eco95")]
    ) &
      supplemental_current[, rowSums(.SD, na.rm = TRUE),
                           .SDcols = c("sco", "eco90", "eco95")] != 0
  ]

  # 2) Full join with factors for selected SUPs (match on common columns)
  factors_keep <- supplemental_factors[sup %chin% c("SCO8665","ECO9044","ECO9544")]
  by_cols <- intersect(names(supplemental_current), names(factors_keep))
  supplemental_current <- merge(supplemental_current, factors_keep, by = by_cols, all.x = TRUE)

  # 3) Apply scaling by SUP in one vectorized shot (no loop)
  #    scale = sco for SCO8665, eco90 for ECO9044, eco95 for ECO9544; otherwise 1
  scale_factor <- supplemental_current[, fcase(
    sup == "SCO8665", sco,
    sup == "ECO9044", eco90,
    sup == "ECO9544", eco95,
    default = 1
  )]
  scale_factor <- fcoalesce(scale_factor, 1)

  cols_to_scale <- c("liability","total_premium","subsidy_amount","producer_premium","indemnity")
  for (cl in cols_to_scale) {
    if (cl %in% names(supplemental_current)) {
      supplemental_current[, (cl) := get(cl) * scale_factor]
    }
  }

  # 4) Sum within keys
  keys <- c("commodity_year","state_code","county_code","commodity_code",
            "type_code","practice_code","unit_structure_code",
            "insurance_plan_code","coverage_level_percent")

  supplemental_current <- supplemental_current[
    ,
    .(
      liability        = sum(liability,        na.rm = TRUE),
      total_premium    = sum(total_premium,    na.rm = TRUE),
      subsidy_amount   = sum(subsidy_amount,   na.rm = TRUE),
      producer_premium = sum(producer_premium, na.rm = TRUE),
      indemnity        = sum(indemnity,        na.rm = TRUE)
    ),
    by = keys
  ]

  # 5) Append base 'base_policy_data' rows (selected columns)
  to_bind <- copy(base_policy_data)[, .(
    commodity_year, state_code, county_code, commodity_code, type_code, practice_code,
    unit_structure_code, insurance_plan_code, coverage_level_percent,
    liability, total_premium, subsidy_amount, producer_premium, indemnity, revenue
  )]
  supplemental_current <- rbindlist(list(supplemental_current, to_bind), fill = TRUE)

  # 6) Final aggregation + label
  supplemental_current <- supplemental_current[
    ,
    .(
      revenue          = sum(revenue,          na.rm = TRUE),
      liability        = sum(liability,        na.rm = TRUE),
      total_premium    = sum(total_premium,    na.rm = TRUE),
      subsidy_amount   = sum(subsidy_amount,   na.rm = TRUE),
      producer_premium = sum(producer_premium, na.rm = TRUE),
      indemnity        = sum(indemnity,        na.rm = TRUE),
      combination      = "Basic+CURRENT"
    ),
    by = keys
  ]

  return(supplemental_current)

}


#' Aggregate supplemental full-participation results
#'
#' Given selected \code{sup} labels, sum their monetary fields, append base
#' outcomes, and produce a final rollup by policy keys with a descriptive
#' \code{combination} label.
#'
#' @details
#' The function self-filters \code{supplemental_factors} to the provided
#' \code{supplemental_pick} (after dropping empties), aggregates within keys,
#' appends base outcomes, and re-aggregates.
#'
#' @param base_policy_data \link[data.table]{data.table}. Base-policy outcomes.
#' @param supplemental_factors \link[data.table]{data.table}. Results from
#'   \code{\link{compute_supplemental_factors}}.
#' @param supplemental_pick Character vector of \code{sup} labels to include.
#'
#' @return A \link[data.table]{data.table} aggregated by the policy keys with:
#'   \code{revenue}, \code{liability}, \code{total_premium}, \code{subsidy_amount},
#'   \code{producer_premium}, \code{indemnity}, and \code{combination}.
#'
#' @import data.table
#' @export
compute_supplemental_full <- function(
    base_policy_data,supplemental_factors,supplemental_pick){

  supplemental_pick <- supplemental_pick[order(supplemental_pick,decreasing = TRUE)]
  supplemental_pick <- supplemental_pick[!supplemental_pick %in% ""]

  if (length(supplemental_pick)) supplemental_factors <- supplemental_factors[sup %chin% supplemental_pick]

  # keys used for grouping
  keys <- c("commodity_year","state_code","county_code","commodity_code",
            "type_code","practice_code","unit_structure_code",
            "insurance_plan_code","coverage_level_percent")

  # aggregate supplemental factors for the chosen SCO/ECO
  scoecoii <- copy(supplemental_factors)[
    ,
    .(
      liability        = sum(liability,        na.rm = TRUE),
      total_premium    = sum(total_premium,    na.rm = TRUE),
      subsidy_amount   = sum(subsidy_amount,   na.rm = TRUE),
      producer_premium = sum(producer_premium, na.rm = TRUE),
      indemnity        = sum(indemnity,        na.rm = TRUE)
    ),
    by = keys
  ]

  # bind base policy rows
  to_bind <- copy(base_policy_data)[, .(
    commodity_year, state_code, county_code, commodity_code,
    type_code, practice_code, unit_structure_code,
    insurance_plan_code, coverage_level_percent,
    liability, total_premium, subsidy_amount, producer_premium, indemnity, revenue
  )]

  scoecoii <- rbindlist(list(scoecoii, to_bind), use.names = TRUE, fill = TRUE)

  # final aggregation + label
  scoecoii <- scoecoii[
    ,
    .(
      revenue          = sum(revenue,          na.rm = TRUE),
      liability        = sum(liability,        na.rm = TRUE),
      total_premium    = sum(total_premium,    na.rm = TRUE),
      subsidy_amount   = sum(subsidy_amount,   na.rm = TRUE),
      producer_premium = sum(producer_premium, na.rm = TRUE),
      indemnity        = sum(indemnity,        na.rm = TRUE),
      combination      = paste0("Basic+",paste0(supplemental_pick,collapse = "+"))
    ),
    by = keys
  ]

  scoecoii[combination %in% "Basic+SCO8665+",combination := "Basic+SCO8665"]

  return(scoecoii)
}

#' Compute incremental supplemental results at an adoption rate
#'
#' Build an incremental scenario by scaling \code{SCO8665} supplemental dollars
#' by a user-specified adoption rate, aggregating by keys, and appending base
#' outcomes.
#'
#' @param base_policy_data \link[data.table]{data.table}. Base-policy outcomes.
#' @param supplemental_factors \link[data.table]{data.table}. Output from
#'   \code{\link{compute_supplemental_factors}} filtered to \code{sup == "SCO8665"}.
#' @param adoption_rate Numeric. Percentage (e.g., \code{10} for 10\%) used to
#'   scale incremental supplemental amounts.
#'
#' @return A \link[data.table]{data.table} aggregated by the policy keys with:
#'   \code{revenue}, \code{liability}, \code{total_premium}, \code{subsidy_amount},
#'   \code{producer_premium}, \code{indemnity}, and \code{combination}.
#'
#' @import data.table
#' @export
compute_supplemental_incremental <- function(base_policy_data,supplemental_factors,adoption_rate){

  agents_data <- unique(copy(base_policy_data)[
    , c("commodity_year","state_code","county_code","commodity_code","type_code",
        "practice_code","unit_structure_code","insurance_plan_code","coverage_level_percent"), with = FALSE])

  agents_data <- agents_data[supplemental_factors,on = intersect(names(supplemental_factors), names(agents_data)),nomatch = 0]

  # 1) Scale selected fields by alt/100 only for SUP == "SCO8665"
  cols <- c("liability","total_premium","subsidy_amount","producer_premium","indemnity")
  agents_data[, (cols) := lapply(.SD, function(v) v * (adoption_rate / 100)), .SDcols = cols]

  # 2) Aggregate within keys
  keys <- c("commodity_year","state_code","county_code","commodity_code",
            "type_code","practice_code","unit_structure_code",
            "insurance_plan_code","coverage_level_percent")

  agents_data <- agents_data[
    ,
    .(
      liability        = sum(liability,        na.rm = TRUE),
      total_premium    = sum(total_premium,    na.rm = TRUE),
      subsidy_amount   = sum(subsidy_amount,   na.rm = TRUE),
      producer_premium = sum(producer_premium, na.rm = TRUE),
      indemnity        = sum(indemnity,        na.rm = TRUE)
    ),
    by = keys
  ]

  # 3) Append base data rows (selected columns)
  to_bind <- copy(base_policy_data)[, .(
    commodity_year, state_code, county_code, commodity_code,
    type_code, practice_code, unit_structure_code,
    insurance_plan_code, coverage_level_percent,
    liability, total_premium, subsidy_amount, producer_premium, indemnity, revenue
  )]

  agents_data <- rbindlist(list(agents_data, to_bind), use.names = TRUE, fill = TRUE)

  # 4) Final aggregation + label
  label <- paste0("Basic+ALT", sprintf("%03d", adoption_rate))
  agents_data <- agents_data[
    ,
    .(
      revenue          = sum(revenue,          na.rm = TRUE),
      liability        = sum(liability,        na.rm = TRUE),
      total_premium    = sum(total_premium,    na.rm = TRUE),
      subsidy_amount   = sum(subsidy_amount,   na.rm = TRUE),
      producer_premium = sum(producer_premium, na.rm = TRUE),
      indemnity        = sum(indemnity,        na.rm = TRUE),
      combination      = label
    ),
    by = keys
  ]

  return(agents_data)
}


#' Dispatcher: simulate supplemental outcomes for one draw
#'
#' Orchestrate the full supplemental simulation workflow for a given crop year
#' and draw: build the agent panel, compute base-policy results, generate
#' supplemental factors, assemble \emph{Current}, \emph{Full}, and
#' \emph{Incremental} scenarios, and write the combined results to disk.
#'
#' @details
#' The pipeline:
#' \enumerate{
#'   \item \code{\link{build_agent_simulation_data}} to construct the panel.
#'   \item \code{\link{compute_base_policy_outcomes}} for base outcomes.
#'   \item \code{\link{study_scenarios}} to enumerate offerings/mixes.
#'   \item Load SCO/ECO ADM; filter to \code{commodity_year == year}; average
#'         \code{base_rate} by key; drop invalid/zero rates.
#'   \item Loop offerings through \code{\link{compute_supplemental_factors}}.
#'   \item Build scenarios:
#'         \itemize{
#'           \item \emph{Current}: \code{\link{compute_supplemental_current}}.
#'           \item \emph{Full}: \code{\link{compute_supplemental_full}}.
#'           \item \emph{Incremental}: \code{\link{compute_supplemental_incremental}}.
#'         }
#'   \item Aggregate base-only results, \code{rbind} all scenarios, and save as
#'         \code{simXXX.rds} in \code{output_directory}.
#' }
#'
#' If \code{output_directory} is \code{NULL}, it defaults to
#' \code{file.path(study_environment$wd$dir_sim, year)} (ensure \code{study_environment$wd$dir_sim}
#' exists in the calling environment).
#'
#' @param sim Integer. Draw number used in data building and the filename.
#' @param year Integer. Crop year.
#' @param agents_directory Character. Directory for cleaned agents data.
#' @param cleaned_rma_sco_and_eco_adm_file_path Character. Path to RDS of SCO/ECO ADM
#'   with join keys and \code{base_rate}. Default:
#'   \code{"data/cleaned_rma_sco_and_eco_adm.rds"}.
#' @param output_directory Character or \code{NULL}. Where to write results;
#'   see Details for default behavior.
#'
#' @return Invisibly writes \code{simXXX.rds} to \code{output_directory}.
#'
#' @import data.table
#' @export
dispatcher_supplemental_simulation <- function(
    sim,
    year,
    agents_directory = "data/cleaned_agents_data",
    cleaned_rma_sco_and_eco_adm_file_path ="data/cleaned_rma_sco_and_eco_adm.rds",
    output_directory = NULL){
  #--------------------------------------------------------------
  # Build agent simulation panel                              ####
  cleaned_agents_data <- build_agent_simulation_data(year = year,sim  = sim,agents_directory = agents_directory)

  #--------------------------------------------------------------
  # Compute base-policy outcomes                              ####
  base_policy_data <- compute_base_policy_outcomes(cleaned_agents_data)
  #--------------------------------------------------------------
  # Get study scenarios                                       ####
  scenarios <- study_scenarios(year)
  #--------------------------------------------------------------
  # Isolate and prep ADM for the commodity year               ####
  year_adm <- readRDS(cleaned_rma_sco_and_eco_adm_file_path)[commodity_year %in% year];gc()
  year_adm <- merge(unique(base_policy_data[, c("state_code","county_code","commodity_code","type_code","practice_code"), with = FALSE]),
                    year_adm,by = c("state_code","county_code","commodity_code","type_code","practice_code"),all.x = TRUE)
  year_adm <- year_adm[
    ,.(base_rate = mean(base_rate,na.rm=T)),
    by = c("state_code","county_code","commodity_code","type_code","practice_code","insurance_plan_code","coverage_level_percent")]
  year_adm <- year_adm[!base_rate %in% c(NA,Inf,Inf,NaN,0)]
  #--------------------------------------------------------------
  # Compute supplemental-policy factors                       ####
  supplemental_factors <- data.table::rbindlist(
    lapply(
      1:nrow(scenarios$offerings),
      function(endors){
        tryCatch({
          # endors <- 2

          dt <- compute_supplemental_factors(
            base_policy = copy(base_policy_data),
            adm = year_adm[insurance_plan_code %in% scenarios$offerings[endors,"insurance_plan_code"]],
            plan    = scenarios$offerings[endors, insurance_plan_code],
            subsidy = scenarios$offerings[endors, Subsidy_factor],
            trigger = scenarios$offerings[endors, Trigger])

          return(dt)

        }, error = function(e){return(NULL)})

      }),fill = TRUE)
  rm(year_adm);gc()

  #--------------------------------------------------------------
  # Compute supplemental-policy - current policy environment  ####
  supplemental_current <- compute_supplemental_current(base_policy_data,supplemental_factors)

  #--------------------------------------------------------------
  # Compute supplemental-policy-full                          ####

  supplemental_full <- data.table::rbindlist(
    lapply(
      1:nrow(scenarios$full_participation),
      function(i){
        tryCatch({
          scoecoii <- compute_supplemental_full(
            base_policy_data     = base_policy_data,
            supplemental_factors = supplemental_factors[sup %chin% unlist(scenarios$full_participation[i])],
            supplemental_pick    = unlist(scenarios$full_participation[i]))
          return(scoecoii)
        }, error = function(e){return(NULL)})
      }),fill = TRUE);gc()

  #--------------------------------------------------------------
  # Compute supplemental-policy-incremental                   ####

  supplemental_incremental <- data.table::rbindlist(
    lapply(c(5,10,15,20,25,30,35,40,45,50,60,70,80,90,100),
           function(adoption_rate){
             tryCatch({
               # alt <- 10
               supplemental_incremental <- compute_supplemental_incremental(
                 base_policy_data = base_policy_data,
                 supplemental_factors = copy(supplemental_factors)[sup %in% "SCO8665"],
                 adoption_rate = adoption_rate)
               return(supplemental_incremental)
             }, error = function(e){return(NULL)})
           }),fill = TRUE)

  #--------------------------------------------------------------
  # FINAL                                                     ####
  base_policy_data <- base_policy_data[,.(
    revenue = sum(revenue,na.rm=T),
    liability = sum(liability,na.rm=T),
    total_premium = sum(total_premium,na.rm=T),
    subsidy_amount = sum(subsidy_amount,na.rm=T),
    producer_premium = sum(producer_premium,na.rm=T),
    indemnity = sum(indemnity,na.rm=T),
    combination="Basic only"),
    by = c("commodity_year","state_code","county_code","commodity_code","type_code","practice_code","unit_structure_code","insurance_plan_code","coverage_level_percent")]

  final_results <- rbind(base_policy_data,supplemental_current,supplemental_full,supplemental_incremental)

  rm(base_policy_data,supplemental_current,supplemental_full,supplemental_incremental,
     supplemental_factors,cleaned_agents_data,scenarios); gc()

  if (!dir.exists(output_directory)) dir.create(output_directory, recursive = TRUE)
  save_path <- file.path(output_directory, paste0("sim", formatC(sim, width = 3, flag = "0"), ".rds"))
  saveRDS(as.data.table(final_results), file = save_path)

  rm(final_results); gc()
  #--------------------------------------------------------------

}
