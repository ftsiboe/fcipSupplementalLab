#' Build County-Level Supplemental Insurance Eligibility, Offering, and Adoption Dynamics
#'
#' @description
#' Builds a county-year-commodity panel describing **(i) eligibility**, **(ii) offering
#' availability**, and **(iii) adoption intensity** for selected FCIP supplemental insurance
#' products.
#'
#' This function expects a **pre-processed SOB/TPU-style** dataset that already contains
#' stub-level adoption measures in:
#' \itemize{
#'   \item \code{supplemental_plan}: a stub identifier (e.g., \code{"sco"}, \code{"eco90"})
#'   \item \code{eligible_acres}: the base-policy eligible/insured acres denominator
#'   \item \code{supplemental_area}: the supplemental endorsed/acquired acres numerator
#' }
#'
#' The function then augments these adoption totals with county-level **availability flags**
#' derived from RMA ADM insurance offer records (\code{A00030_InsuranceOffer}).
#'
#' @details
#' For each stub name in \code{supplemental_stubs}, the function:
#' \enumerate{
#'   \item Filters \code{sob} to records whose \code{supplemental_plan} matches the stub name
#'   (via \code{grepl()}) and aggregates \code{eligible_acres} and \code{supplemental_area} to
#'   \code{commodity_year} x \code{state_code} x \code{county_code} x \code{commodity_code}
#'   (retaining \code{supplemental_plan}).
#'
#'   \item Downloads ADM \code{A00030_InsuranceOffer} for each year present in \code{sob}
#'   (via \code{rfcip::get_adm_data()}) and constructs two binary flags:
#'   \itemize{
#'     \item \code{eligible}: equals 1 if any record exists with
#'     \code{insurance_plan_code} in \code{eligible_codes}
#'     \item \code{offered}: equals 1 if any record exists with
#'     \code{insurance_plan_code} in \code{offering_codes}
#'   }
#'   These indicators are collapsed using a \code{max()} rule within each county-year-commodity.
#'   A companion 'all-commodities' version (\code{commodity_code = 0}) is also appended.
#'
#'   \item Builds a complete county shell from ADM \code{A00440_County} for each year and
#'   crosses it with the set of \code{(commodity_year, commodity_code)} pairs present in the
#'   ADM availability table so counties with zero insured/adopted acres are retained.
#'
#'   \item Left-joins availability flags to the county shell, then joins adoption totals from
#'   \code{sob}. Missing numeric fields are set to 0. The function enforces basic bounds such
#'   as \code{supplemental_acres <= eligible_acres}.
#' }
#'
#' The returned object stacks all stub-specific panels into one long table with a
#' \code{supplemental_plan} identifier.
#'
#' @param sob A \code{data.frame} or \code{data.table} containing (at minimum)
#' \code{commodity_year}, \code{state_code}, \code{county_code}, \code{commodity_code},
#' \code{supplemental_plan}, \code{eligible_acres}, and \code{supplemental_area}.
#'
#' @param supplemental_stubs Optional named list defining which supplemental products to
#' include. Each element must be a list with:
#' \itemize{
#'   \item \code{offering_codes}: insurance plan codes indicating the product is offered in ADM
#'   \item \code{eligible_codes}: insurance plan codes indicating the county is eligible in ADM
#' }
#' If \code{NULL}, a default set is constructed for SCO, STAX, MP, ECO (90/95), HIP-WI, PACE,
#' and FIP-SI (with plan-code ranges as specified in the function body).
#'
#' @return A \code{data.table} with one row per
#' \code{commodity_year} x \code{state_code} x \code{county_code} x \code{commodity_code}
#' x \code{supplemental_plan} containing:
#' \itemize{
#'   \item \code{eligible}: 0/1 indicator for eligibility in ADM
#'   \item \code{supplemental_offered}: 0/1 indicator for offering availability in ADM
#'   \item \code{eligible_acres}: aggregated base-policy eligible/insured acres denominator
#'   \item \code{supplemental_acres}: aggregated supplemental acres (\code{supplemental_area})
#'   \item \code{county_fips}: 5-digit county FIPS (character)
#'   \item \code{supplemental_plan}: stub name (character)
#' }
#'
#' @export
build_supplemental_adoption_dynamics <- function(
    sob,
    supplemental_stubs = NULL) {
  
  if(is.null(supplemental_stubs)){
    supplemental_stubs <- list(
      sco   = list(offering_codes = 31:33 , eligible_codes = c(1:3,90)),
      stax  = list(offering_codes = 35:36 , eligible_codes = c(35:36,1:3,90,4:6)),
      mp    = list(offering_codes = 16:17 , eligible_codes = c(16:17,1:3,90,4:6)),
      eco90 = list(offering_codes = 87:89 , eligible_codes = c(1:3,90)),
      eco95 = list(offering_codes = 87:89 , eligible_codes = c(1:3,90)),
      hipwi = list(offering_codes = 37    , eligible_codes = c(1:3,90)),
      pace  = list(offering_codes = 26:28 , eligible_codes = c(1:3,90)),
      fipsi = list(offering_codes = 38    , eligible_codes = c(1:3,90))
    )
  }
  
  sob <- data.table::as.data.table(sob)
  
  req_cols <- c("commodity_year","state_code","county_code","commodity_code","supplemental_plan","eligible_acres","supplemental_area")
  missing_cols <- setdiff(req_cols, names(sob))
  if (length(missing_cols)) {
    stop("SOB/TPU file missing required columns: ",
         paste(missing_cols, collapse = ", "))
  }
  
  res <- lapply(1:length(supplemental_stubs), function(stub) {
    tryCatch({
      
      # stub <- 1
      eligible_codes <- supplemental_stubs[[stub]]$eligible_codes
      offering_codes  <- supplemental_stubs[[stub]]$offering_codes
      
      # ---- SOB/TPU: compute adoption acres & aggregate --------------------------
      supplemental_acres <- data.table::copy(sob)[
        grepl(names(supplemental_stubs)[stub],supplemental_plan),
        .(eligible_acres = sum(eligible_acres, na.rm = TRUE),
          supplemental_area = sum(supplemental_area, na.rm = TRUE)),
        by = c("commodity_year","state_code","county_code","commodity_code","supplemental_plan")]
      
      supplemental_acres[supplemental_area > eligible_acres, supplemental_area:= eligible_acres]
      supplemental_acres <- supplemental_acres[!eligible_acres %in% 0]
      
      # ---- ADM availability by year ---------------------------------------------
      
      adm <- lapply(sort(unique(sob$commodity_year)), function(y) {
        dt <- as.data.table(rfcip::get_adm_data(y, dataset = "A00030_InsuranceOffer"))
        # coerce types we'll use
        cols <- c("commodity_year","state_code","county_code","commodity_code","insurance_plan_code")
        dt[, (cols) := lapply(.SD, function(x) as.numeric(as.character(x))), .SDcols = cols]
        dt <- dt[insurance_plan_code %in% c(eligible_codes, offering_codes)]
        
        # per spec: ECO exists 2021+
        out <- rbind(
          dt[insurance_plan_code %in% eligible_codes,
             .(plan = "eligible", available = 1L),
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
        
        for (nm in c("eligible","offered")) if (!nm %in% names(out)) out[, (nm) := 0L]
        
        out[]
      })
      adm <- rbindlist(adm, fill = TRUE)
      
      adm_all <- data.table::copy(adm)[
        , .(
          eligible = as.integer(max(eligible,  na.rm = TRUE)),
          offered = as.integer(max(offered,  na.rm = TRUE))
        ),
        by = .(commodity_year, state_code, county_code)
      ][,commodity_code := 0]
      
      adm <- rbind(adm,adm_all)
      
      for (nm in c("eligible","offered")){
        adm[is.na(get(nm)), (nm) := 0L]
      }
      
      # ---- Build county shell (from ADM A00440_County) -----------------------------
      ctab <- lapply(sort(unique(sob$commodity_year)), function(y) {
        x <- data.table::as.data.table(rfcip::get_adm_data(y, dataset = "A00440_County"))
        
        need <- c("state_code","county_code","county_name")
        miss <- setdiff(need, names(x))
        if (length(miss)) stop("A00440_County missing columns: ", paste(miss, collapse = ", "))
        
        x <- unique(x[, ..need])
        
        # coerce codes to integer (defensive)
        x[, state_code  := as.numeric(as.character(state_code))]
        x[, county_code := as.numeric(as.character(county_code))]
        
        x[, commodity_year := as.numeric(y)]
        x[]
      })
      ctab <- data.table::rbindlist(ctab, fill = TRUE)
      
      # (year, commodity) support from ADM availability table
      yc <- unique(adm[, .(commodity_year, commodity_code)])
      yc[, commodity_year := as.numeric(commodity_year)]
      yc[, commodity_code := as.numeric(commodity_code)]
      
      # Cross join within year (safe cartesian product)
      yc[, .tmp_key := 1L]
      ctab[, .tmp_key := 1L]
      shell <- merge(yc, ctab, by = c("commodity_year", ".tmp_key"), allow.cartesian = TRUE)[, .tmp_key := NULL]
      
      # Build county_fips (5-digit) from codes
      shell[, county_fips := paste0(
        stringr::str_pad(state_code,  2, pad = "0"),
        stringr::str_pad(county_code, 3, pad = "0")
      )]
      
      # ---- Join availability to shell, then to SOB adoption ---------------------
      avail <- merge(
        shell, adm,
        by = c("commodity_year","state_code","county_code","commodity_code"),
        all.x = TRUE)
      
      # binary flags: replace NAs with 0
      for (nm in c("eligible","offered")){
        if (!nm %in% names(avail)) avail[, (nm) := 0L]
        avail[is.na(get(nm)), (nm) := 0L]
      }
      
      # join supplemental_acres
      dt <- lapply(unique(supplemental_acres$supplemental_plan), function(nm) {
        dt <- merge(
          avail, supplemental_acres[supplemental_plan %in% nm],
          by = c("commodity_year","state_code","county_code","commodity_code"),
          all.x = TRUE)
        dt[,supplemental_plan := nm]
        dt[]
      })
      dt <- rbindlist(dt, fill = TRUE)
      
      # replace missing numeric with 0 for acres
      num_cols <- c("eligible_acres","supplemental_area")
      for (nm in num_cols) {
        if (!nm %in% names(dt)) dt[, (nm) := 0]
        dt[!is.finite(get(nm)) | is.na(get(nm)), (nm) := 0]
      }
      
      # ensure single row per key; aggregate deterministically
      dt <- dt[
        , .(
          eligible            = as.integer(max(eligible,  na.rm = TRUE)),
          supplemental_offered = as.integer(max(offered,  na.rm = TRUE)),
          eligible_acres       = sum(eligible_acres, na.rm = TRUE),
          supplemental_acres   = sum(supplemental_area, na.rm = TRUE)
        ),
        by = .(commodity_year, state_code, county_code, county_name, commodity_code,supplemental_plan)
      ]
      
      # rebuild FIPS for convenience
      dt[, county_fips := paste0(stringr::str_pad(state_code, 2, pad = "0"),
                                 stringr::str_pad(county_code, 3, pad = "0"))]
      
      dt[supplemental_acres > eligible_acres, supplemental_acres:= eligible_acres]
      
      dt[]
    }, error=function(e){NULL})
  })
  
  res <- Filter(Negate(is.null), res)
  if (!length(res)) return(data.table::data.table())
  res <- data.table::rbindlist(res, fill = TRUE)
  
  res[]
}


#' Build Supplemental Eligible Acres and Supplemental Area from RMA SOB-TPU
#'
#' @description
#' Constructs a stacked, analysis-ready table of **supplemental adoption acres**
#' (\code{supplemental_area}) and companion **eligible/base insured acres**
#' (\code{eligible_acres}) for a user-specified set of FCIP supplemental products.
#'
#' This function is an orchestrator: it calls \code{allocate_supplemental_area()} for each
#' requested product family (e.g., SCO, ECO, PACE), then row-binds the results into one
#' long \code{data.table}. It does **not** compute shares; it returns levels that can be
#' converted to shares downstream if desired.
#'
#' @details
#' Output content depends on which plan codes are supplied in \code{supplemental_codes}.
#' For each included product, \code{allocate_supplemental_area()} aggregates:
#' \itemize{
#'   \item \code{supplemental_area} from \code{endorsed_acres} for the supplemental codes, and
#'   \item \code{eligible_acres} from \code{insured_acres} for the corresponding base-policy codes,
#' }
#' at the requested disaggregation grain. Product labels are stored in
#' \code{supplemental_plan} (e.g., \code{"sco"}, \code{"eco90"}, \code{"eco95"}).
#'
#' @param sob A \code{data.table} of cleaned SOB-TPU records containing both base and
#' supplemental policies. Must include \code{insured_acres}, \code{endorsed_acres},
#' \code{insurance_plan_code}, and the identifier columns referenced in \code{disaggregates}.
#'
#' @param supplemental_codes Integer vector of supplemental insurance plan codes to include.
#' Defaults to a comprehensive set of FCIP supplemental products.
#'
#' @param disaggregates Optional character vector defining the primary aggregation grain.
#' Defaults to \code{c("commodity_year","state_code","county_code","commodity_code")}.
#'
#' @return A \code{data.table} containing the requested identifiers, a
#' \code{supplemental_plan} label, and the level variables \code{eligible_acres} and
#' \code{supplemental_area}.
#'
#' @export
get_supplemental_area <- function(
    sob,
    supplemental_codes = c(
      31:33,   # SCO
      87:89,   # ECO
      35:36,   # STAX
      16:17,   # MP
      67:69,   # MCO
      26:28,   # PACE
      37,      # HIP-WI
      38       # FIP-SI
    ),
    disaggregates = NULL){
  
  if(is.null(disaggregates)){
    disaggregates = c("commodity_year", "state_code", "county_code", "commodity_code")
  }

  res <- list()
  
  if(any(31:33 %in% supplemental_codes)){
    res[[length(res)+1]] <- allocate_supplemental_area(
      sob                       = sob,
      disaggregates             = disaggregates,
      base_policy_codes         = c(1,2,3,90),
      supplemental_codes        = 31:33,
      base_anchor               = 30,
      supplemental_name         = "sco",
      track_base_plan           = TRUE,
      track_base_coverage_level = TRUE,
      split_by_coverage_level   = TRUE)
  }
  
  if(any(87:89 %in% supplemental_codes)){
    res[[length(res)+1]] <- allocate_supplemental_area(
      sob                       = sob,
      disaggregates             = disaggregates,
      base_policy_codes         = c(1,2,3,90),
      supplemental_codes        = 87:89,
      base_anchor               = 86,
      supplemental_name         = "eco",
      track_base_plan           = TRUE,
      track_base_coverage_level = TRUE,
      split_by_coverage_level   = TRUE)
  }
  
  if(any(26:28 %in% supplemental_codes)){
    res[[length(res)+1]] <- allocate_supplemental_area(
      sob                       = sob,
      disaggregates             = disaggregates,
      base_policy_codes         = c(1,2,3,90),
      supplemental_codes        = 26:28,
      base_anchor               = 25,
      supplemental_name         = "pace",
      track_base_plan           = TRUE,
      track_base_coverage_level = TRUE,
      split_by_coverage_level   = FALSE)
  }
  
  if(any(37 %in% supplemental_codes)){
    res[[length(res)+1]] <- allocate_supplemental_area(
      sob                       = sob,
      disaggregates             = disaggregates,
      base_policy_codes         = c(1,2,3,90),
      supplemental_codes        = 37,
      supplemental_name         = "hipwi",
      track_base_plan           = FALSE,
      track_base_coverage_level = FALSE,
      split_by_coverage_level   = FALSE)

  }
  
  if(any(38 %in% supplemental_codes)){
    res[[length(res)+1]] <- allocate_supplemental_area(
      sob                       = sob,
      disaggregates             = disaggregates,
      base_policy_codes         = c(1,2,3,90),
      supplemental_codes        = 38,
      supplemental_name         = "fipsi",
      track_base_plan           = FALSE,
      track_base_coverage_level = FALSE,
      split_by_coverage_level   = FALSE)
  }
  
  if(any(16:17 %in% supplemental_codes)){
    
    res[[length(res)+1]] <- allocate_supplemental_area(
      sob                       = sob,
      disaggregates             = disaggregates,
      base_policy_codes         = c(1,2,3,90,16:17,4:6),
      supplemental_codes        = 16:17,
      supplemental_name         = "mp",
      track_base_plan           = FALSE,
      track_base_coverage_level = TRUE,
      split_by_coverage_level   = FALSE)

  }
  
  if(any(35:36 %in% supplemental_codes)){
    res[[length(res)+1]] <- allocate_supplemental_area(
      sob                       = sob,
      disaggregates             = disaggregates,
      base_policy_codes         = c(35:36,1:3,90,4:6),
      supplemental_codes        = 35:36,
      supplemental_name         = "stax",
      track_base_plan           = FALSE,
      track_base_coverage_level = TRUE,
      split_by_coverage_level   = FALSE)

  }
  
  res <- data.table::rbindlist(res,fill = TRUE)
  
  return(res)
}


#' Allocate Supplemental Area and Eligible Acres for a Supplemental Product Stub
#'
#' @description
#' Aggregates supplemental endorsed acres (\code{supplemental_area}) for selected
#' \code{supplemental_codes} and aggregates base insured acres (\code{eligible_acres}) for
#' selected \code{base_policy_codes}, at a common disaggregation grain, returning a long
#' table keyed by identifiers and a \code{supplemental_plan} label.
#'
#' This is a low-level helper used by \code{get_supplemental_area()} and
#' \code{build_supplemental_adoption_dynamics()} to construct product- and
#' coverage-specific adoption totals.
#'
#' @details
#' The function implements the following steps:
#' \enumerate{
#'   \item **Supplemental aggregation (numerator):** Filters \code{sob} to
#'   \code{supplemental_codes} and aggregates \code{endorsed_acres} to form
#'   \code{supplemental_area} at the requested disaggregation level. Depending on settings,
#'   aggregation may additionally track the base plan (\code{insurance_plan_code}) and/or the
#'   base coverage level (\code{coverage_level_percent}).
#'
#'   \item **Plan-code anchoring (optional):** If \code{base_anchor} is supplied,
#'   \code{insurance_plan_code} is shifted by subtracting the anchor value. This is typically
#'   used to map stacked-plan codes back onto underlying base plans (e.g., SCO 31-33 -> base
#'   plans 1-3 by subtracting 30).
#'
#'   \item **Coverage-specific labeling (optional):** If \code{split_by_coverage_level = TRUE},
#'   constructs \code{supplemental_plan} labels using \code{coverage_level_percent} (e.g.,
#'   \code{"eco90"}, \code{"eco95"}) and removes \code{coverage_level_percent} from the
#'   aggregation grain so coverage becomes encoded in the label rather than the keys.
#'
#'   \item **Base aggregation (denominator):** Aggregates \code{insured_acres} for
#'   \code{base_policy_codes} to form \code{eligible_acres} at the same disaggregation grain.
#'
#'   \item **Join and bounds:** Inner-joins numerator and denominator on shared identifiers,
#'   drops rows with \code{eligible_acres == 0}, and enforces \code{supplemental_area <= eligible_acres}.
#' }
#'
#' @param sob A \code{data.table} containing the full SOB-TPU extract (base and supplemental
#' records). Must include \code{endorsed_acres}, \code{insured_acres},
#' \code{insurance_plan_code}, and the identifier columns referenced in \code{disaggregates}.
#'
#' @param base_policy_codes Integer vector of base plan codes defining the denominator
#' acreage universe used to compute \code{eligible_acres}.
#'
#' @param supplemental_codes Integer vector of SOB insurance plan codes identifying the
#' supplemental product(s) whose endorsed acres form \code{supplemental_area}.
#'
#' @param supplemental_name Character scalar used to construct \code{supplemental_plan} when
#' \code{split_by_coverage_level = FALSE}.
#'
#' @param disaggregates Optional character vector defining the primary aggregation grain.
#' Defaults to \code{c("commodity_year","state_code","county_code","commodity_code")}.
#'
#' @param base_anchor Optional integer used to re-anchor stacked plan codes to base plan codes.
#'
#' @param track_base_plan Logical. If \code{TRUE}, aggregates separately by
#' \code{insurance_plan_code} (after optional anchoring).
#'
#' @param track_base_coverage_level Logical. If \code{TRUE}, aggregates separately by
#' \code{coverage_level_percent}.
#'
#' @param split_by_coverage_level Logical. If \code{TRUE}, encode coverage into
#' \code{supplemental_plan} labels (e.g., \code{"eco90"}) rather than retaining
#' \code{coverage_level_percent} as a key column.
#'
#' @return A \code{data.table} containing the requested identifiers, \code{supplemental_plan},
#' and the level variables \code{supplemental_area} and \code{eligible_acres}.
#'
#' @export
allocate_supplemental_area <- function(
    sob,
    base_policy_codes,
    supplemental_codes,
    supplemental_name,
    disaggregates = NULL,
    base_anchor = NULL,
    track_base_plan = TRUE,
    track_base_coverage_level = TRUE,
    split_by_coverage_level   = FALSE
){
  
  if(is.null(disaggregates)){
    disaggregates = c("commodity_year", "state_code", "county_code", "commodity_code")
  }
  
  # Supplemental acres
  disaggregates_updated <- disaggregates
  if(track_base_plan){
    disaggregates_updated <- unique(c(disaggregates_updated,"insurance_plan_code"))
  }
  if(track_base_coverage_level){
    disaggregates_updated <- unique(c(disaggregates_updated,"coverage_level_percent"))
  }
  
  data <- sob[
    insurance_plan_code %in% supplemental_codes,
    .(supplemental_area = sum(endorsed_acres, na.rm = TRUE)),
    by = c(disaggregates_updated)
  ]
  
  if(!is.null(base_anchor)){
    data[, insurance_plan_code := insurance_plan_code - base_anchor]
  }
  
  if(split_by_coverage_level){
    data[, supplemental_plan := paste0(supplemental_name, round(coverage_level_percent * 100))]
    disaggregates_updated <- disaggregates_updated[!disaggregates_updated %in% "coverage_level_percent"]
  }else{
    data[, supplemental_plan := supplemental_name]
  }
  data <- data[, .(supplemental_area = sum(supplemental_area, na.rm = TRUE)),
               by = c(disaggregates_updated, "supplemental_plan")]
  
  base_data <- sob[
    insurance_plan_code %in% base_policy_codes,
    .(eligible_acres = sum(insured_acres, na.rm = TRUE)),
    by = c(disaggregates_updated)]
  
  data <- data[base_data,on = intersect(names(base_data), names(data)),nomatch = 0]
  data <- data[!supplemental_plan %in% NA]
  data[supplemental_area > eligible_acres, supplemental_area:= eligible_acres]
  data <- data[!eligible_acres %in% 0]
  
  # data <- data |> tidyr::spread(supplemental_plan, supplemental_area)
  # data <- data |> tidyr::gather(supplemental_plan, supplemental_area, names(data)[grepl("_area$",names(data))])
  # data$supplemental_area <- ifelse(data$supplemental_area %in% c(NA,Inf,-Inf,NaN),0,data$supplemental_area)
  # data <- data |> tidyr::spread(coverage_name, supplemental_area)
  
  return(data)
}