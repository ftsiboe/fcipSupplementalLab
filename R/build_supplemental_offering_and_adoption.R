#' Build panel of supplemental insurance availability (offering) and adoption (acres)
#'
#' Builds a county-year-commodity panel that combines:
#' (i) availability flags for APH, SCO, and ECO (ECO90/ECO95), sourced from the
#' RMA ADM dataset `A00030_InsuranceOffer`, and
#' (ii) adoption/acreage measures computed from cleaned RMA SOB/TPU records.
#'
#' The function aggregates SOB/TPU records to county-year-commodity totals, then
#' constructs a full county shell using `urbnmapr` and merges in ADM availability
#' and SOB/TPU adoption. ECO availability is only populated for years >= 2021.
#' Missing availability flags and acreage measures are replaced with 0.
#'
#' @param sob data.frame or data.table. Cleaned SOB/TPU-like micro data containing
#'   at least `commodity_year`, `state_code`, `county_code`, `commodity_code`,
#'   `insured_acres`, and indicator columns `sco`, `eco90`, `eco95` (typically 0/1).
#'
#' @details
#' **Adoption acres (SOB/TPU)**:
#' `sco`, `eco90`, and `eco95` are converted to adopted acres by multiplying each
#' indicator by `insured_acres`, then summing within
#' `(commodity_year, state_code, county_code, commodity_code)` for years >= 2015.
#'
#' **Availability (ADM)**:
#' Availability is derived from `A00030_InsuranceOffer` by insurance plan code:
#' \itemize{
#'   \item APH/Yield plan family: 1, 2, 3, 90 -> `avail_aph`
#'   \item SCO endorsements: 31--33 -> `avail_sco`
#'   \item ECO endorsements: 87--89 -> `avail_eco90` and `avail_eco95` (years >= 2021 only)
#' }
#' Availability is aggregated to binary flags (max).
#'
#' **County shell**:
#' The output includes all U.S. counties (from `urbnmapr`) crossed with all
#' `(commodity_year, commodity_code)` pairs present in the ADM availability table.
#' Counties with no offering or no adoption for a given key will have zeros.
#'
#' @return A `data.table` with one row per
#' `(commodity_year, state_code, county_code, commodity_code)` containing:
#' \itemize{
#'   \item Keys: `commodity_year`, `state_code`, `county_code`, `commodity_code`
#'   \item Convenience: `county_fips` (character, 5-digit FIPS)
#'   \item Availability flags: `avail_aph`, `avail_sco`, `avail_eco90`, `avail_eco95` (0/1)
#'   \item Adoption acres: `insured_acres`, `sco`, `eco90`, `eco95`
#' }
#'
#' @import data.table
#' @importFrom stringr str_pad
#' @importFrom rfcip get_adm_data
#' @importFrom urbnmapr get_urbn_map
#' @export
build_supplemental_offering_and_adoption <- function(sob) {

  sob <- data.table::as.data.table(sob)

  req_cols <- c("commodity_year","state_code","county_code","commodity_code",
                "insured_acres","sco","eco90","eco95")
  missing_cols <- setdiff(req_cols, names(sob))
  if (length(missing_cols)) {
    stop("SOB/TPU data missing required columns: ",
         paste(missing_cols, collapse = ", "))
  }

  # ------------------------------------------------------------------
  # 1) SOB/TPU: coerce indicators to 0/1, compute adopted acres, aggregate
  # ------------------------------------------------------------------
  sob[, insured_acres := suppressWarnings(as.numeric(insured_acres))]

  # defensively coerce endorsements to 0/1 (treat NA/non-1 as 0)
  sob[, sco  := as.integer(suppressWarnings(as.numeric(sco))  %in% 1)]
  sob[, eco90:= as.integer(suppressWarnings(as.numeric(eco90))%in% 1)]
  sob[, eco95:= as.integer(suppressWarnings(as.numeric(eco95))%in% 1)]

  sob[, sco   := insured_acres * sco]
  sob[, eco90 := insured_acres * eco90]
  sob[, eco95 := insured_acres * eco95]

  sob <- sob[
    commodity_year >= 2015,
    .(
      insured_acres = sum(insured_acres, na.rm = TRUE),
      sco           = sum(sco,           na.rm = TRUE),
      eco90         = sum(eco90,         na.rm = TRUE),
      eco95         = sum(eco95,         na.rm = TRUE)
    ),
    by = .(commodity_year, state_code, county_code, commodity_code)
  ]

  # ------------------------------------------------------------------
  # 2) ADM availability by year (A00030_InsuranceOffer)
  # ------------------------------------------------------------------
  PLAN_APH <- c(1L, 2L, 3L, 90L)   # APH/Yield plan families
  PLAN_SCO <- 31L:33L             # SCO endorsements
  PLAN_ECO <- 87L:89L             # ECO endorsements family

  years <- sort(unique(sob$commodity_year))

  adm_list <- lapply(years, function(y) {

    dt <- data.table::as.data.table(rfcip::get_adm_data(y, dataset = "A00030_InsuranceOffer"))

    # Robustly map year column to commodity_year if needed
    # Required columns for availability
    need <- c("commodity_year","state_code","county_code","commodity_code","insurance_plan_code")
    miss <- setdiff(need, names(dt))
    if (length(miss)) stop("ADM A00030_InsuranceOffer missing columns: ", paste(miss, collapse = ", "))

    # Coerce types
    dt[, (need) := lapply(.SD, function(x) suppressWarnings(as.integer(as.character(x)))),
       .SDcols = need]

    dt <- dt[insurance_plan_code %in% c(PLAN_APH, PLAN_SCO, PLAN_ECO)]

    out <- rbind(
      dt[insurance_plan_code %in% PLAN_APH,
         .(plan = "avail_aph",  avail = 1L),
         by = .(commodity_year, state_code, county_code, commodity_code)],
      dt[insurance_plan_code %in% PLAN_SCO,
         .(plan = "avail_sco",  avail = 1L),
         by = .(commodity_year, state_code, county_code, commodity_code)]
    )

    # ECO exists 2021+
    if (y >= 2021) {
      eco <- dt[insurance_plan_code %in% PLAN_ECO,
                .(commodity_year, state_code, county_code, commodity_code)]
      if (nrow(eco)) {
        eco <- unique(eco)
        # If A00030 doesn't distinguish 90 vs 95, both are marked available.
        eco[, `:=`(avail_eco90 = 1L, avail_eco95 = 1L)]
        eco_long <- data.table::melt(
          eco,
          id.vars = c("commodity_year","state_code","county_code","commodity_code"),
          variable.name = "plan",
          value.name = "avail"
        )
        out <- rbind(out, eco_long, fill = TRUE)
      }
    }

    # Cast to wide; collapse by max -> binary flags
    out <- data.table::dcast(
      out,
      commodity_year + state_code + county_code + commodity_code ~ plan,
      value.var = "avail",
      fun.aggregate = function(z) as.integer(max(z %in% 1L))
    )

    out[]
  })

  adm <- data.table::rbindlist(adm_list, fill = TRUE)

  # Ensure flag columns exist and are 0/1
  for (nm in c("avail_aph","avail_sco","avail_eco90","avail_eco95")) {
    if (!nm %in% names(adm)) adm[, (nm) := 0L]
    adm[is.na(get(nm)), (nm) := 0L]
    adm[, (nm) := as.integer(get(nm) %in% 1L)]
  }

  # ------------------------------------------------------------------
  # 3) County shell (robust FIPS extraction) and cartesian product
  # ------------------------------------------------------------------
  counties <- urbnmapr::get_urbn_map(map = "counties", sf = TRUE)

  fips_col <- intersect(c("county_fips","geoid","GEOID","fips","FIPS"), names(counties))
  if (length(fips_col) < 1L) {
    stop("Could not find a county FIPS/GEOID column in urbnmapr counties map. Columns: ",
         paste(names(counties), collapse = ", "))
  }
  # prefer county_fips if available
  if ("county_fips" %in% fips_col) fips_col <- "county_fips" else fips_col <- fips_col[[1L]]

  ctab <- data.table::data.table(
    county_fips = as.character(counties[[fips_col]])
  )
  # normalize to 5-digit
  ctab[, county_fips := stringr::str_pad(county_fips, 5, pad = "0")]
  ctab <- unique(ctab)

  # year-commodity pairs present in ADM availability
  yc <- unique(adm[, .(commodity_year, commodity_code)])

  # safe cartesian join: all counties x (year, commodity)
  yc[, key := 1L]
  ctab[, key := 1L]
  shell <- merge(yc, ctab, by = "key", allow.cartesian = TRUE)[, key := NULL]
  yc[, key := NULL]
  ctab[, key := NULL]

  # split FIPS once into numeric codes
  shell[, state_code  := as.integer(substr(county_fips, 1, 2))]
  shell[, county_code := as.integer(substr(county_fips, 3, 5))]
  shell[, county_fips := NULL]

  # ------------------------------------------------------------------
  # 4) Join availability to shell, then adoption (SOB)
  # ------------------------------------------------------------------
  avail <- merge(
    shell, adm,
    by = c("commodity_year","state_code","county_code","commodity_code"),
    all.x = TRUE
  )

  for (nm in c("avail_aph","avail_sco","avail_eco90","avail_eco95")) {
    if (!nm %in% names(avail)) avail[, (nm) := 0L]
    avail[is.na(get(nm)), (nm) := 0L]
    avail[, (nm) := as.integer(get(nm) %in% 1L)]
  }

  dt <- merge(
    avail, sob,
    by = c("commodity_year","state_code","county_code","commodity_code"),
    all.x = TRUE
  )

  # replace missing numeric with 0 for acres
  num_cols <- c("insured_acres","sco","eco90","eco95")
  for (nm in num_cols) {
    if (!nm %in% names(dt)) dt[, (nm) := 0]
    dt[!is.finite(get(nm)) | is.na(get(nm)), (nm) := 0]
  }

  # ensure single row per key; aggregate deterministically
  dt <- dt[
    , .(
      avail_aph   = as.integer(max(avail_aph,   na.rm = TRUE)),
      avail_sco   = as.integer(max(avail_sco,   na.rm = TRUE)),
      avail_eco90 = as.integer(max(avail_eco90, na.rm = TRUE)),
      avail_eco95 = as.integer(max(avail_eco95, na.rm = TRUE)),
      insured_acres = sum(insured_acres, na.rm = TRUE),
      sco           = sum(sco,           na.rm = TRUE),
      eco90         = sum(eco90,         na.rm = TRUE),
      eco95         = sum(eco95,         na.rm = TRUE)
    ),
    by = .(commodity_year, state_code, county_code, commodity_code)
  ]

  # rebuild FIPS for convenience
  dt[, county_fips := paste0(
    stringr::str_pad(state_code, 2, pad = "0"),
    stringr::str_pad(county_code, 3, pad = "0")
  )]

  dt[]
}

