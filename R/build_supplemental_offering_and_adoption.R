#' Build panel of supplemental insurance availability (offering) and adoption (acres)
#'
#' Creates a county-year-commodity panel with availability flags for APH/SCO/ECO90/ECO95
#' and adoption/acreage measures from RMA SOB/TPU. Availability is sourced from the
#' RMA ADM (A00030_InsuranceOffer). ECO availability applies starting in 2021.
#'
#' @param cleaned_rma_sobtpu_file_path Character. Path to cleaned RMA SOB/TPU RDS.
#'   Default: "data/cleaned_rma_sobtpu.rds".
#' @param output_directory Character. Directory to save output RDS; created if missing.
#'   Default: "data".
#'
#' @return Invisibly returns the output file path. Also prints a brief summary.
#' @details
#' Output columns:
#' - commodity_year, state_code, county_code, commodity_code, county_fips
#' - avail_aph, avail_sco, avail_eco90, avail_eco95 (0/1 flags)
#' - insured_acres, sco, eco90, eco95 (adopted acres)
#'
#' Availability aggregation uses max() (binary). Acreage aggregation uses sum().
#' Missing numeric values are replaced with 0.
#'
#' @examples
#' \dontrun{
#'   path <- build_supplemental_offering_and_adoption()
#'   readRDS(path)[1:5]
#' }
#' @import data.table stringr rfcip urbnmapr
#' @export
build_supplemental_offering_and_adoption <- function(
    cleaned_rma_sobtpu_file_path = "data/cleaned_rma_sobtpu.rds",
    output_directory = "data"
) {
  # ---- Setup & checks -------------------------------------------------------
  if (!file.exists(cleaned_rma_sobtpu_file_path)) {
    stop("Input RDS not found: ", cleaned_rma_sobtpu_file_path)
  }

  sob <- readRDS(cleaned_rma_sobtpu_file_path)
  sob <- as.data.table(sob)
  req_cols <- c("commodity_year","state_code","county_code","commodity_code",
                "insured_acres","sco","eco90","eco95")
  missing_cols <- setdiff(req_cols, names(sob))
  if (length(missing_cols)) {
    stop("SOB/TPU file missing required columns: ",
         paste(missing_cols, collapse = ", "))
  }

  # ---- SOB/TPU: compute adoption acres & aggregate --------------------------
  sob[, sco   := as.numeric(insured_acres) * as.numeric(sco)]
  sob[, eco90 := as.numeric(insured_acres) * as.numeric(eco90)]
  sob[, eco95 := as.numeric(insured_acres) * as.numeric(eco95)]

  sob <- sob[commodity_year >= 2015,
             .(insured_acres = sum(insured_acres, na.rm = TRUE),
               sco           = sum(sco,           na.rm = TRUE),
               eco90         = sum(eco90,         na.rm = TRUE),
               eco95         = sum(eco95,         na.rm = TRUE)),
             by = .(commodity_year, state_code, county_code, commodity_code)]

  # ---- ADM availability by year ---------------------------------------------
  PLAN_APH <- c(1L, 2L, 3L, 90L)     # APH/Yield plan families
  PLAN_SCO <- 31L:33L                # SCO endorsements
  PLAN_ECO <- 87L:89L                # ECO endorsements (ECO90/ECO95 both from 87:89 family)

  years <- sort(unique(sob$commodity_year))
  adm_list <- lapply(years, function(y) {
    dt <- as.data.table(rfcip::get_adm_data(y, dataset = "A00030_InsuranceOffer"))
    # coerce types we'll use
    cols <- c("commodity_year","state_code","county_code","commodity_code","insurance_plan_code")
    dt[, (cols) := lapply(.SD, function(x) as.numeric(as.character(x))), .SDcols = cols]
    dt <- dt[insurance_plan_code %in% c(PLAN_APH, PLAN_SCO, PLAN_ECO)]

    # per spec: ECO exists 2021+
    out <- rbind(
      dt[insurance_plan_code %in% PLAN_APH,
         .(plan = "avail_aph", avail = 1L),
         by = .(commodity_year, state_code, county_code, commodity_code)],
      dt[insurance_plan_code %in% PLAN_SCO,
         .(plan = "avail_sco", avail = 1L),
         by = .(commodity_year, state_code, county_code, commodity_code)]
    )

    if (y >= 2021) {
      eco <- dt[insurance_plan_code %in% PLAN_ECO,
                .(commodity_year, state_code, county_code, commodity_code)]
      if (nrow(eco)) {
        eco[, `:=`(avail_eco90 = 1L, avail_eco95 = 1L)]
        eco <- unique(eco)
        eco_long <- melt(eco,
                         id.vars = c("commodity_year","state_code","county_code","commodity_code"),
                         variable.name = "plan", value.name = "avail")
        eco_long[, plan := as.character(plan)]
        out <- rbind(out, eco_long, fill = TRUE)
      }
    }
    # cast to wide, then collapse by max()
    out <- dcast(out,
                 commodity_year + state_code + county_code + commodity_code ~ plan,
                 value.var = "avail",
                 fun.aggregate = function(z) as.integer(max(z %in% 1)))
    out[]
  })
  adm <- rbindlist(adm_list, fill = TRUE)

  # Ensure flag columns exist
  for (nm in c("avail_aph","avail_sco","avail_eco90","avail_eco95")) {
    if (!nm %in% names(adm)) adm[, (nm) := 0L]
    adm[is.na(get(nm)), (nm) := 0L]
  }

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
  for (nm in c("avail_aph","avail_sco","avail_eco90","avail_eco95")) {
    if (!nm %in% names(avail)) avail[, (nm) := 0L]
    avail[is.na(get(nm)), (nm) := 0L]
  }

  # join SOB
  dt <- merge(avail, sob,
              by = c("commodity_year","state_code","county_code","commodity_code"),
              all.x = TRUE)

  # replace missing numeric with 0 for acres
  num_cols <- c("insured_acres","sco","eco90","eco95")
  for (nm in num_cols) {
    if (!nm %in% names(dt)) dt[, (nm) := 0]
    dt[!is.finite(get(nm)) | is.na(get(nm)), (nm) := 0]
  }

  # ensure single row per key; aggregate deterministically
  dt <- dt[
    , .(
      avail_aph  = as.integer(max(avail_aph,  na.rm = TRUE)),
      avail_sco  = as.integer(max(avail_sco,  na.rm = TRUE)),
      avail_eco90= as.integer(max(avail_eco90,na.rm = TRUE)),
      avail_eco95= as.integer(max(avail_eco95,na.rm = TRUE)),
      insured_acres = sum(insured_acres, na.rm = TRUE),
      sco           = sum(sco,           na.rm = TRUE),
      eco90         = sum(eco90,         na.rm = TRUE),
      eco95         = sum(eco95,         na.rm = TRUE)
    ),
    by = .(commodity_year, state_code, county_code, commodity_code)
  ]

  # rebuild FIPS for convenience
  dt[, county_fips := paste0(stringr::str_pad(state_code, 2, pad = "0"),
                             stringr::str_pad(county_code, 3, pad = "0"))]

  # ---- Save & return --------------------------------------------------------
  if (!dir.exists(output_directory)) dir.create(output_directory, recursive = TRUE)
  save_path <- file.path(output_directory, "supplemental_offering_and_adoption.rds")
  saveRDS(dt, save_path)

  msg <- sprintf("Built panel %s-%s, rows = %s. Saved: %s",
                 min(dt$commodity_year), max(dt$commodity_year),
                 format(nrow(dt), big.mark = ","), save_path)
  message(msg)
  invisible(save_path)
}
