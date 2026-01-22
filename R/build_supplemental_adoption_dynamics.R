#' Build County-Level Supplemental Insurance Eligibility, Offering, and Adoption Dynamics
#'
#' @description
#' Builds a county-year-commodity panel describing **(i) eligibility**, **(ii) offering
#' availability**, and **(iii) adoption intensity** for selected FCIP supplemental insurance
#' products. The function combines:
#' \itemize{
#'   \item a cleaned SOB/TPU-derived dataset containing base insured acres and (optionally)
#'   supplemental adoption measures (either as shares or as acre totals), and
#'   \item RMA Actuarial Data Master (ADM) insurance-offer records from
#'   \code{A00030_InsuranceOffer} to determine whether a supplemental product is eligible and/or
#'   offered in each county-year-commodity.
#' }
#'
#' @details
#' For each entry in \code{supplemental_stubs}, the function performs the following steps:
#'
#' \enumerate{
#'   \item **Input validation:** checks that `sob` contains the required identifiers and the
#'   requested supplemental measure column (e.g., \code{sco_share} or \code{stax_area}).
#'
#'   \item **Compute adoption acres from SOB/TPU:**
#'   \itemize{
#'     \item If the measure name contains \code{"_share"}, it computes
#'     \code{adoption_acres = insured_acres * measure} (interpreting the measure as a share of
#'     base insured acres).
#'     \item Otherwise, it treats the measure as an acreage quantity and sets
#'     \code{adoption_acres = measure} and then sets \code{insured_acres = adoption_acres}
#'     (so the "insured_acres" field in the stub-specific output reflects the relevant acreage
#'     universe for that product).
#'   }
#'   It then aggregates adoption acres and insured acres to
#'   \code{commodity_year by state_code by county_code by commodity_code} and restricts to
#'   \code{commodity_year >= 2015}.
#'
#'   \item **Construct ADM availability flags:** for each year present in \code{sob},
#'   downloads ADM \code{A00030_InsuranceOffer} via \code{rfcip::get_adm_data()}, filters to
#'   the union of eligibility and offering plan codes for the stub, and creates two binary
#'   county-level indicators:
#'   \itemize{
#'     \item \code{eligibile}: equals 1 if any record exists with a plan in \code{eligibile_codes}
#'     \item \code{offered}: equals 1 if any record exists with a plan in \code{offering_codes}
#'   }
#'   (Indicators are collapsed using a max() rule within each county-year-commodity.)
#'
#'   \item **Build a complete county shell:** creates a complete county shell using
#'   \code{urbnmapr::get_urbn_map("counties")} and crosses all counties with the set of
#'   \code{(commodity_year, commodity_code)} pairs present in ADM so that counties with zero
#'   insured/adopted acres are retained in the final panel.
#'
#'   \item **Join and finalize:** left-joins ADM availability to the county shell, then joins
#'   adoption totals from SOB/TPU. Missing numeric fields are set to 0. The result is then
#'   aggregated deterministically to one row per county-year-commodity and labeled with
#'   \code{supplemental_plan} (the stub name) and a reconstructed \code{county_fips}.
#' }
#'
#' The returned object stacks all stub-specific panels into one long table with a
#' \code{supplemental_plan} identifier.
#'
#' @param sob A data.frame or \code{data.table} containing (at minimum)
#' \code{commodity_year}, \code{state_code}, \code{county_code}, \code{commodity_code},
#' \code{insured_acres}, and the supplemental measure columns referenced by
#' \code{supplemental_stubs[[*]]$measure}. Measures may be shares (ending in \code{"_share"})
#' or acreage totals (e.g., \code{stax_area}, \code{mp_area}).
#'
#' @param supplemental_stubs Optional named list defining which supplemental products to
#' include and how to interpret them. Each element must be a list with:
#' \itemize{
#'   \item \code{measure}: column name in \code{sob} to use (share or acres)
#'   \item \code{offering_codes}: insurance plan codes indicating the product is offered in ADM
#'   \item \code{eligibile_codes}: insurance plan codes indicating the county is eligible in ADM
#' }
#' If \code{NULL}, a default set is constructed for SCO, STAX, MP, ECO (90/95), HIP-WI, PACE,
#' and FIP-SI.
#'
#' @return A \code{data.table} with one row per
#' \code{commodity_year by state_code by county_code bycommodity_code by supplemental_plan}
#' containing:
#' \itemize{
#'   \item \code{eligibile}: 0/1 indicator for eligibility in ADM
#'   \item \code{supplemental_offered}: 0/1 indicator for offering availability in ADM
#'   \item \code{insured_acres}: aggregated acreage universe used for the stub
#'   \item \code{supplemental_acres}: aggregated adoption acres (derived from shares or acres)
#'   \item \code{county_fips}: 5-digit county FIPS (character)
#'   \item \code{supplemental_plan}: stub name (character)
#' }
#' @export
build_supplemental_adoption_dynamics <- function(
    sob,
    supplemental_stubs = NULL) {
  
  if(is.null(supplemental_stubs)){
    supplemental_stubs <- list(
      sco   = list(measure = "sco_share"   , offering_codes = 31:33 , eligibile_codes = c(1:3,90)),
      stax  = list(measure = "stax_area"   , offering_codes = 35:36 , eligibile_codes = c(35:36,1:3,90)),
      mp    = list(measure = "mp_area"     , offering_codes = 16:17 , eligibile_codes = c(16:17)),
      eco90 = list(measure = "eco90_share" , offering_codes = 87:89 , eligibile_codes = c(1:3,90)),
      eco95 = list(measure = "eco95_share" , offering_codes = 87:89 , eligibile_codes = c(1:3,90)),
      hipwi = list(measure = "hipwi_share" , offering_codes = 37    , eligibile_codes = c(1:3,90)),
      pace  = list(measure = "pace_share"  , offering_codes = 26:28 , eligibile_codes = c(1:3,90)),
      fipsi = list(measure = "fipsi_share" , offering_codes = 38    , eligibile_codes = c(1:3,90))
    )
  }
  
  sob <- data.table::as.data.table(sob)
  
  res <- lapply(1:length(supplemental_stubs), function(stub) {
    # stub <- 1
    measure <- supplemental_stubs[[stub]]$measure
    eligibile_codes <- supplemental_stubs[[stub]]$eligibile_codes
    offering_codes <- supplemental_stubs[[stub]]$offering_codes
    
    req_cols <- c("commodity_year","state_code","county_code","commodity_code","insured_acres",measure)
    missing_cols <- setdiff(req_cols, names(sob))
    if (length(missing_cols)) {
      stop("SOB/TPU file missing required columns: ",
           paste(missing_cols, collapse = ", "))
    }
    
    # ---- SOB/TPU: compute adoption acres & aggregate --------------------------
    supplemental_acres <- copy(sob)
    
    if(grepl("_share",measure)){
      supplemental_acres[, adoption_acres := as.numeric(insured_acres) * as.numeric(get(measure))]
    }else{
      supplemental_acres[, adoption_acres := as.numeric(get(measure))]
      supplemental_acres[, insured_acres := adoption_acres]
    }
 
    supplemental_acres <- supplemental_acres[
      commodity_year >= 2015,
      .(insured_acres  = sum(insured_acres, na.rm = TRUE),
        adoption_acres = sum(adoption_acres, na.rm = TRUE)),
      by = .(commodity_year, state_code, county_code, commodity_code)]
    
    # ---- ADM availability by year ---------------------------------------------
    
    adm <- lapply(sort(unique(sob$commodity_year)), function(y) {
      dt <- as.data.table(rfcip::get_adm_data(y, dataset = "A00030_InsuranceOffer"))
      # coerce types we'll use
      cols <- c("commodity_year","state_code","county_code","commodity_code","insurance_plan_code")
      dt[, (cols) := lapply(.SD, function(x) as.numeric(as.character(x))), .SDcols = cols]
      dt <- dt[insurance_plan_code %in% c(eligibile_codes, offering_codes)]
      
      # per spec: ECO exists 2021+
      out <- rbind(
        dt[insurance_plan_code %in% eligibile_codes,
           .(plan = "eligibile", available = 1L),
           by = .(commodity_year, state_code, county_code, commodity_code)],
        dt[insurance_plan_code %in% offering_codes,
           .(plan = "offered", available = 1L),
           by = .(commodity_year, state_code, county_code, commodity_code)]
      )
      
      # cast to wide, then collapse by max()
      out <- dcast(out,
                   commodity_year + state_code + county_code + commodity_code ~ plan,
                   value.var = "available",
                   fun.aggregate = function(z) as.integer(max(z %in% 1)))
      out[]
    })
    adm <- rbindlist(adm, fill = TRUE)
    
    adm[,supplemental_plan := names(supplemental_stubs)[stub]]
    
    # ---- Build county shell (from urbnmapr) -----------------------------------
    counties <- urbnmapr::get_urbn_map(map = "counties", sf = TRUE)
    ctab <- data.table(
      county_fips = as.character(counties$county_fips),
      county_name = as.character(counties$county_name)
    )
    # all FIPS * all (year, commodity) present in ADM
    yc <- unique(adm[, .(commodity_year, commodity_code)])
    shell <- cbind(
      yc[rep(seq_len(nrow(yc)), each = nrow(ctab))],
      ctab[rep(seq_len(nrow(ctab)), times = nrow(yc))]
    )
    # split FIPS once into numeric codes
    shell[, state_code  := as.integer(substr(county_fips, 1, 2))]
    shell[, county_code := as.integer(substr(county_fips, 3, 5))]
    shell[, county_fips := NULL]
    
    # ---- Join availability to shell, then to SOB adoption ---------------------
    avail <- merge(shell, adm,
                   by = c("commodity_year","state_code","county_code","commodity_code"),
                   all.x = TRUE)
    
    # binary flags: replace NAs with 0
    for (nm in c("eligibile","offered")){
      if (!nm %in% names(avail)) avail[, (nm) := 0L]
      avail[is.na(get(nm)), (nm) := 0L]
    }
    
    # join supplemental_acres
    dt <- merge(avail, supplemental_acres,
                by = c("commodity_year","state_code","county_code","commodity_code"),
                all.x = TRUE)
    
    # replace missing numeric with 0 for acres
    num_cols <- c("insured_acres","adoption_acres")
    for (nm in num_cols) {
      if (!nm %in% names(dt)) dt[, (nm) := 0]
      dt[!is.finite(get(nm)) | is.na(get(nm)), (nm) := 0]
    }
    
    # ensure single row per key; aggregate deterministically
    dt <- dt[
      , .(
        eligibile            = as.integer(max(eligibile,  na.rm = TRUE)),
        supplemental_offered = as.integer(max(offered,  na.rm = TRUE)),
        insured_acres        = sum(insured_acres, na.rm = TRUE),
        supplemental_acres   = sum(adoption_acres, na.rm = TRUE)
      ),
      by = .(commodity_year, state_code, county_code, commodity_code)
    ]
    
    dt[,supplemental_plan := names(supplemental_stubs)[stub]]
    # rebuild FIPS for convenience
    dt[, county_fips := paste0(stringr::str_pad(state_code, 2, pad = "0"),
                               stringr::str_pad(county_code, 3, pad = "0"))]
    
    dt[]
    
  })
  
  res <- rbindlist(res, fill = TRUE)
  
  res[]
}


#' Build a Base-Policy Panel with Supplemental Adoption Measures from RMA SOB-TPU
#'
#' @description
#' Constructs an analysis-ready panel of **base insurance outcomes** and appends
#' measures of **supplemental insurance adoption** and/or **supplemental acreage
#' totals** for a user-specified set of FCIP supplemental products.
#'
#' The function first aggregates base (individual-based) insurance outcomes from
#' `sob_full`, harmonizing plan code 90 to base plan 1 where applicable. It then
#' sequentially augments this base panel with supplemental adoption measures
#' computed via `get_supplemental_shares()` and with direct acreage aggregates
#' for selected area-based products.
#'
#' @details
#' Output content depends on which supplemental plan codes are supplied:
#'
#' \itemize{
#'   \item **SCO (31-33):** Adds `sco_share`, measuring the share of base insured
#'   acres stacked with SCO.
#'
#'   \item **ECO (87-89):** Adds coverage-specific adoption measures
#'   `eco90_share` and `eco95_share`.
#'
#'   \item **PACE (26-28):** Adds `pace_share`.
#'
#'   \item **HIP-WI (37):** Adds `hipwi_share` (not disaggregated by base plan or
#'   coverage level).
#'
#'   \item **FIP-SI (38):** Adds `fipsi_share`.
#'
#'   \item **MP (16-17):** Adds `mp_area`, representing aggregated acres insured
#'   under Margin Protection.
#'
#'   \item **STAX (35-36):** Adds `stax_area`, representing aggregated acres
#'   insured under STAX.
#' }
#'
#' Adoption shares are based on `endorsed_acres` in the numerator and base-policy
#' `insured_acres` in the denominator. Area-based products (MP and STAX) are
#' returned as acreage totals rather than shares.
#'
#' After all merges, the function enforces basic bounds:
#' \itemize{
#'   \item Non-finite values are set to 0
#'   \item Negative values are truncated to 0
#'   \item Share variables are capped at 1
#' }
#'
#' @param sob_full A `data.table` of cleaned SOB-TPU records containing both base
#'   and supplemental policies. Must include insured acres, endorsed acres,
#'   financial totals, and standard FCIP identifier columns.
#'
#' @param supplemental_codes Integer vector of supplemental insurance plan codes
#'   to include. Defaults to a comprehensive set of FCIP supplemental products.
#'
#' @param disaggregates Optional character vector defining the primary analysis
#'   grain for adoption measures. Defaults to commodity-year by county by
#'   commodity/type/practice.
#'
#' @return A `data.table` containing base insured acreage and financial totals at
#' the base-policy aggregation grain, augmented with one or more supplemental
#' adoption variables (ending in `"_share"`) and/or supplemental acreage totals
#' (ending in `"_area"`).
#'
#' @export
get_supplemental_adoption <- function(
    sob_full,
    supplemental_codes = c(
      31:33,   # SCO
      87:89,   # ECO
      35:36,   # Stacked Inc Prot Plan
      16:17,   # MP
      67:69,   # MCO
      26:28,   # PACE
      37,      # HIP-WI
      38       # FIP-SI
    ),
    disaggregates = NULL){
  
  if(is.null(disaggregates)){
    disaggregates = c("commodity_year", "state_code", "county_code", "commodity_code", "type_code", "practice_code")
  }
  
  # Base data aggregation
  sob_base <- copy(sob_full)[insurance_plan_code %in% 90, insurance_plan_code:= 1][
    insurance_plan_code %in% c(1:3),
    .(
      insured_acres        = sum(insured_acres,   na.rm = TRUE),
      liability_amount     = sum(liability_amount,             na.rm = TRUE),
      total_premium_amount = sum(total_premium_amount,         na.rm = TRUE),
      subsidy_amount       = sum(subsidy_amount,               na.rm = TRUE),
      indemnity_amount     = sum(indemnity_amount,             na.rm = TRUE)
    ),
    by = c("commodity_year", "state_code", "county_code", "commodity_code", "type_code", "practice_code",
           "unit_structure_code", "insurance_plan_code", "coverage_type_code", "coverage_level_percent")
  ][
    is.finite(insured_acres) & insured_acres > 0
  ]
  
  data <- copy(sob_base)[
    ,
    .(
      insured_acres        = sum(insured_acres,   na.rm = TRUE),
      liability_amount     = sum(liability_amount,             na.rm = TRUE),
      total_premium_amount = sum(total_premium_amount,         na.rm = TRUE),
      subsidy_amount       = sum(subsidy_amount,               na.rm = TRUE),
      indemnity_amount     = sum(indemnity_amount,             na.rm = TRUE)
    ),
    by = c("commodity_year", "state_code", "county_code", "commodity_code", "type_code", "practice_code",
           "unit_structure_code", "insurance_plan_code", "coverage_type_code", "coverage_level_percent")
  ] 
  
  if(any(31:33 %in% supplemental_codes)){
    sco_data <- get_supplemental_shares(
      sob_base                = sob_base,
      sob_full                = sob_full,
      disaggregates           = c("commodity_year", "state_code", "county_code", "commodity_code", "type_code", "practice_code"),
      supplemental_codes      = 31:33,
      base_anchor             = 30,
      supplemental_name       = "sco",
      track_base_plan           = TRUE,
      track_base_coverage_level = TRUE,
      split_by_coverage_level   = FALSE)
    data <- merge(data, sco_data, by = intersect(names(data), names(sco_data)), all = TRUE)
  }
  
  if(any(87:89 %in% supplemental_codes)){
    eco_data <- get_supplemental_shares(
      sob_base                  = sob_base,
      sob_full                  = sob_full,
      disaggregates             = c("commodity_year", "state_code", "county_code", "commodity_code", "type_code", "practice_code"),
      supplemental_codes        = 87:89,
      base_anchor               = 86,
      supplemental_name         = "eco",
      track_base_plan           = TRUE,
      track_base_coverage_level = TRUE,
      split_by_coverage_level   = TRUE)
    data <- merge(data, eco_data, by = intersect(names(data), names(eco_data)), all = TRUE)
  }
  
  if(any(26:28 %in% supplemental_codes)){
    pace_data <- get_supplemental_shares(
      sob_base                = sob_base,
      sob_full                = sob_full,
      disaggregates           = c("commodity_year", "state_code", "county_code", "commodity_code", "type_code", "practice_code"),
      supplemental_codes      = 26:28,
      base_anchor             = 25,
      supplemental_name       = "pace",
      track_base_plan           = TRUE,
      track_base_coverage_level = TRUE,
      split_by_coverage_level   = FALSE)
    data <- merge(data, pace_data, by = intersect(names(data), names(pace_data)), all = TRUE)
  }
  
  if(any(37 %in% supplemental_codes)){
    hipwi_data <- get_supplemental_shares(
      sob_base                  = sob_base,
      sob_full                  = sob_full,
      disaggregates             = c("commodity_year", "state_code", "county_code", "commodity_code", "type_code", "practice_code"),
      supplemental_codes        = 37,
      supplemental_name         = "hipwi",
      track_base_plan           = FALSE,
      track_base_coverage_level = FALSE,
      split_by_coverage_level   = FALSE)
    data <- merge(data, hipwi_data, by = intersect(names(data), names(hipwi_data)), all = TRUE)
  }
  
  if(any(38 %in% supplemental_codes)){
    fipsi_data <- get_supplemental_shares(
      sob_base                  = sob_base,
      sob_full                  = sob_full,
      disaggregates             = c("commodity_year", "state_code", "county_code", "commodity_code", "type_code", "practice_code"),
      supplemental_codes        = 38,
      supplemental_name         = "fipsi",
      track_base_plan           = FALSE,
      track_base_coverage_level = FALSE,
      split_by_coverage_level   = FALSE)
    data <- merge(data, fipsi_data, by = intersect(names(data), names(fipsi_data)), all = TRUE)
  }
  
  if(any(16:17 %in% supplemental_codes)){
    mp_data <- sob_full[
      insurance_plan_code %in% 16:17,
      .(
        mp_area = sum(insured_acres,   na.rm = TRUE) + sum(endorsed_acres,   na.rm = TRUE)
      ),
      by = c("commodity_year", "state_code", "county_code", "commodity_code", "type_code", "practice_code",
             "unit_structure_code", "insurance_plan_code", "coverage_type_code", "coverage_level_percent")
    ]
    mp_data[, insurance_plan_code := insurance_plan_code-14]
    data <- merge(data, mp_data, by = intersect(names(data), names(mp_data)), all = TRUE)
    
  }
  
  if(any(35:36 %in% supplemental_codes)){
    stax_data <- sob_full[
      insurance_plan_code %in% 35:36,
      .(
        stax_area = sum(insured_acres,   na.rm = TRUE) + sum(endorsed_acres,   na.rm = TRUE)
      ),
      by = c("commodity_year", "state_code", "county_code", "commodity_code", "type_code", "practice_code",
             "unit_structure_code", "insurance_plan_code", "coverage_type_code", "coverage_level_percent")
    ]
    stax_data[, insurance_plan_code := insurance_plan_code-33]
    data <- merge(data, stax_data, by = intersect(names(data), names(stax_data)), all = TRUE)
  }
  
  for (nn in names(data)[grepl("_area|_share",names(data))]) {
    if (!nn %in% names(data)) next
    data[!is.finite(get(nn)), (nn) := NA_real_]
    data[get(nn) < 0, (nn) := 0]
    data[is.na(get(nn)), (nn) := 0]
  }
  
  for (nn in names(data)[grepl("_share",names(data))]) {
    if (!nn %in% names(data)) next
    data[get(nn) > 1, (nn) := 1]
  }
  
  data <- data[is.finite(insured_acres) & insured_acres > 0]
  return(data)
}


#' Compute Supplemental Product Shares Relative to a Base Insured-Acres Denominator
#'
#' @description
#' Computes **supplemental adoption measures** by expressing supplemental acres as a
#' fraction of **base insured acres** from a companion base dataset. This function is a
#' low-level helper used by `calculate_supplemental_adoption()` to construct product-level
#' uptake variables such as `sco_share`, `pace_share`, `hipwi_share`, or coverage-specific
#' measures like `eco90_share` and `eco95_share`.
#'
#' @details
#' The function implements the following steps:
#'
#' \enumerate{
#'   \item **Supplemental aggregation (numerator):** Filters `sob_full` to
#'   `supplemental_codes` and aggregates `endorsed_acres` to form
#'   `supplemental_area` at the requested disaggregation level. Depending on
#'   user settings, aggregation may additionally track the base plan
#'   (`insurance_plan_code`) and/or the base coverage level
#'   (`coverage_level_percent`).
#'
#'   \item **Plan-code anchoring (optional):** If `base_anchor` is supplied,
#'   `insurance_plan_code` is shifted by subtracting the anchor value. This is
#'   typically used to map stacked-plan codes back onto their underlying base
#'   plans (e.g., SCO codes 31-33 -> base plans 1-3 by subtracting 30).
#'
#'   \item **Base aggregation (denominator):** Aggregates `sob_base` to the
#'   intersection of keys shared with the supplemental aggregation and computes
#'   total `insured_acres` for use as the denominator.
#'
#'   \item **Share construction:** Computes
#'   \code{supplemental_share = supplemental_area / insured_acres} when
#'   `insured_acres > 0`; otherwise assigns `NA`.
#'
#'   \item **De-duplication and reshaping:** If multiple rows exist for the same
#'   identifier set and coverage label, the function collapses duplicates by
#'   taking the **mean** share within each identifier group before reshaping to
#'   wide format.
#' }
#'
#' If `split_by_coverage_level = TRUE`, output columns are labeled by coverage
#' level percent (rounded) and named
#' `<supplemental_name><CL>_share` (e.g., `eco90_share`). In this case,
#' `coverage_level_percent` is removed from merge keys so that shares vary only
#' along the remaining disaggregation dimensions.
#'
#' All non-finite share values (NA, Inf, -Inf, NaN) are coerced to **0** in the
#' final output.
#'
#' @param sob_base A `data.table` containing **base-policy outcomes** used as the
#'   denominator. Must include `insured_acres` and the identifier columns needed
#'   to form merge keys.
#'
#' @param sob_full A `data.table` containing the **full SOB-TPU extract**
#'   (base and supplemental records). Must include `endorsed_acres`,
#'   `insurance_plan_code`, and the identifier columns referenced in
#'   `disaggregates`.
#'
#' @param supplemental_codes Integer vector of SOB insurance plan codes
#'   identifying the supplemental product(s) whose adoption is to be measured.
#'
#' @param supplemental_name Character scalar used to construct output column
#'   names. The suffix `"_share"` is appended internally.
#'
#' @param disaggregates Optional character vector defining the primary
#'   aggregation grain. Defaults to
#'   `c("commodity_year","state_code","county_code","commodity_code",
#'     "type_code","practice_code")`.
#'
#' @param base_anchor Optional integer used to re-anchor stacked plan codes to
#'   base plan codes.
#'
#' @param track_base_plan Logical. If `TRUE`, adoption shares are computed
#'   separately by base insurance plan.
#'
#' @param track_base_coverage_level Logical. If `TRUE`, adoption shares are
#'   computed separately by base coverage level.
#'
#' @param split_by_coverage_level Logical. If `TRUE`, returns separate share
#'   columns by coverage level percent.
#'
#' @return A `data.frame` (resulting from `tidyr` reshaping) containing the
#'   requested identifiers and one or more wide columns ending in `"_share"`.
#' @export
get_supplemental_shares <- function(
    sob_base,
    sob_full,
    supplemental_codes,
    supplemental_name,
    disaggregates = NULL,
    base_anchor = NULL,
    track_base_plan = TRUE,
    track_base_coverage_level = TRUE,
    split_by_coverage_level   = FALSE
){
  
  if(is.null(disaggregates)){
    disaggregates = c("commodity_year", "state_code", "county_code", "commodity_code", "type_code", "practice_code")
  }
  
  # Supplemental acres
  disaggregates_updated <- disaggregates
  if(track_base_plan){
    disaggregates_updated <- unique(c(disaggregates_updated,"insurance_plan_code"))
  }
  if(track_base_coverage_level){
    disaggregates_updated <- unique(c(disaggregates_updated,"coverage_level_percent"))
  }
  
  data <- sob_full[
    insurance_plan_code %in% supplemental_codes,
    .(supplemental_area = sum(endorsed_acres, na.rm = TRUE)),
    by = c(disaggregates_updated)
  ]
  
  if(!is.null(base_anchor)){
    data[, insurance_plan_code := insurance_plan_code - base_anchor]
  }
  
  
  if(split_by_coverage_level){
    data[, coverage_name := paste0(supplemental_name, round(coverage_level_percent * 100))]
    disaggregates_updated <- disaggregates_updated[!disaggregates_updated %in% "coverage_level_percent"]
  }else{
    data[, coverage_name := supplemental_name]
  }
  
  data[, coverage_name := paste0(coverage_name,"_share")]
  
  merg_keys <- intersect(names(sob_base),names(data))
  if(split_by_coverage_level){
    merg_keys <- merg_keys[!merg_keys %in% "coverage_level_percent"]
  }
  
  data <- data[
    sob_base[, .(insured_acres = sum(insured_acres, na.rm = TRUE)),
             by = merg_keys],
    on = merg_keys,
    nomatch = 0
  ]
  
  data[, supplemental_share := fifelse(insured_acres > 0, supplemental_area/insured_acres, NA_real_)]
  
  data <- data[, .(supplemental_share = mean(supplemental_share, na.rm = TRUE)),
               by = c(disaggregates_updated, "coverage_name")]
  
  data <- data |> tidyr::spread(coverage_name, supplemental_share)
  data <- data |> tidyr::gather(coverage_name, supplemental_share, names(data)[grepl("_share$",names(data))])
  data$supplemental_share <- ifelse(data$supplemental_share %in% c(NA,Inf,-Inf,NaN),0,data$supplemental_share)
  data <- data |> tidyr::spread(coverage_name, supplemental_share)
  
  return(data)
}