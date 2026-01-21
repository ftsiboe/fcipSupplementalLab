#' Build SCO/ECO/Area ADM table for a given year (adds SCO88/SCO90)
#'
#' @description
#' Downloads yearly ADM fragments from GitHub Releases for
#' \emph{Supplemental SCO}, \emph{Supplemental ECO}, and \emph{Area} plans,
#' aggregates key parameters by common grouping keys, linearly interpolates
#' SCO rates to 88% and 90% (using AYP and, for years >= 2021, ECO anchors),
#' and returns the cleaned, stacked table.
#'
#' @param year Integer. commodity year (e.g., 2022).
#'
#' @return A \link[data.table]{data.table} containing original SCO/ECO/Area ADM rows
#'   plus synthesized \strong{SCO88} (\code{insurance_plan_code + 10}) and
#'   \strong{SCO90} (\code{insurance_plan_code + 20}) rows with non-invalid
#'   \code{base_rate}.
#'
#' @note
#' Requires internet access. Missing plan files for a year are skipped silently.
#'
#' @import data.table
#' @importFrom utils download.file
#' @export
clean_rma_sco_and_eco_adm <- function(year){
  # year <- 2022  # Helper for ad-hoc runs

  # Build a single ADM table by downloading/reading 3 plan families and stacking them.
  # - "Supplemental_sco" : plan codes 31-33
  # - "Supplemental_eco" : plan codes 87-89
  # - "Area"             : includes AYP (plan codes 4-6), used as anchor for interpolation
  admfull <- data.table::rbindlist(
    lapply(
      c("Supplemental_sco","Supplemental_eco","Area"),
      function(plan){
        tryCatch({
          # plan <- "Supplemental_sco"  # ad-hoc debugging

          # Download the yearly release artifact for this plan family to a temp .rds file
          adm <- tempfile(fileext = ".rds")
          download.file(
            paste0("https://github.com/ftsiboe/USFarmSafetyNetLab/releases/download/adm_compressed/",
                   year,"_",plan,"_plans_adm.rds"),
            adm, mode = "wb", quiet = TRUE)

          # Read ADM into memory
          adm <- readRDS(adm)

          # Numeric ADM parameters to summarize at aggregation points
          parameter_list <-  names(adm)[
            names(adm) %in% c("base_rate","price_volatility_factor","payment_factor",
                              "expected_county_yield","final_county_yield",
                              "projected_price","harvest_price","catastrophic_price",
                              "expected_county_revenue","final_county_revenue" )]

          # Aggregation keys:
          #   - commodity_year
          #   - vectors of column names: "state_code", "county_code", "commodity_code", "type_code", "practice_code" ,
          #                               "unit_structure_code", "insurance_plan_code", "coverage_type_code", "coverage_level_percent"
          #   - area loss band when present
          aggregation_point <-  names(adm)[
            names(adm) %in% c("commodity_year", "state_code", "county_code", "commodity_code", "type_code", "practice_code" ,
                              "unit_structure_code", "insurance_plan_code", "coverage_type_code", "coverage_level_percent",
                              "area_loss_start_percent","area_loss_end_percent")]

          # Collapse ADM to mean values by the chosen keys
          adm <- adm[
            , lapply(.SD, function(x) mean(x, na.rm = TRUE)),
            by = aggregation_point,.SDcols = parameter_list]

          return(adm)

        }, error = function(e){
          # If a particular artifact is missing or malformed, skip it gracefully
          return(NULL)
        })
      }), fill = TRUE)

  # --- Build AYP anchor curves (plans 4-6) for interpolation to 88%/90% ---
  ayp <- admfull[insurance_plan_code %in% c(4:6)]
  # Remove invalid base rates before averaging
  ayp <- ayp[!base_rate %in% c(0,NA,NaN,Inf,-Inf)]
  # Average base rate by year, pool, plan, and coverage level
  ayp <- ayp[,.(base_rate = mean(base_rate,na.rm=T)),
             by = c("commodity_year", "state_code", "county_code", "commodity_code", "type_code",
                    "practice_code" , "insurance_plan_code", "coverage_level_percent")];gc()
  ayp <- as.data.frame(ayp)
  # Create labels such as "ayp85","ayp90" from coverage_level_percent
  ayp$coverage_level_percent <- paste0("ayp",round(ayp$coverage_level_percent*100))
  # Pivot to wide to get columns per level (e.g., ayp85, ayp90)
  ayp <- ayp |> tidyr::spread(coverage_level_percent, base_rate)
  # Shift plan codes to align with SCO plan mapping (AYP 4-6 to 31-33 via +27)
  ayp$insurance_plan_code <- ayp$insurance_plan_code + 27

  # Join AYP anchors onto SCO rows (31-33) to enable linear interpolation
  ayp <- dplyr::full_join(admfull[insurance_plan_code %in% 31:33],ayp)
  # Remove rows with invalid base_rate (keeps data clean for interpolation)
  ayp <- ayp[!base_rate %in% c(0,NA,NaN,Inf,-Inf)]
  # Keep only SCO plan codes in the merged set
  ayp <- ayp[insurance_plan_code %in% 31:33]

  # --- Interpolate SCO to 88% and 90% using AYP anchors ---
  # The interpolation uses a linear step from a baseline (implicitly 86%) to target:
  #   base_rate_ayp = base_rate + (ayp90 - ayp85) * ((target - 86)/(90 - 85))
  # Produces two candidate adjusted base rates: base_rate_ayp for 88 and 90.
  sco88 <- copy(ayp)[,base_rate_ayp := base_rate + (ayp90 - ayp85)*((88-86)/(90-85))]
  sco90 <- copy(ayp)[,base_rate_ayp := base_rate + (ayp90 - ayp85)*((90-86)/(90-85))]

  rm(ayp);gc()

  # --- If ECO exists (year >= 2021), also interpolate against ECO and harmonize ---
  if(year>=2021){

    # ECO at 90% (plans 87-89); retag plan codes to align with SCO plans (-56 to 31-33)
    eco90 <- admfull[(insurance_plan_code %in% 87:89 & round(coverage_level_percent*100) %in% 90)
                     , c("commodity_year", "state_code", "county_code", "commodity_code", "type_code",
                         "practice_code" , "insurance_plan_code","base_rate"), with = FALSE]
    setnames(eco90,old = c("base_rate"),new = c("eco90"))
    eco90[,insurance_plan_code := insurance_plan_code - 56]

    # SCO at 85% (plans 31-33), used as the baseline for ECO-anchored interpolation
    sco85 <- admfull[(insurance_plan_code %in% 31:33 & round(coverage_level_percent*100) %in% 85)
                     , c("commodity_year", "state_code", "county_code", "commodity_code", "type_code",
                         "practice_code" , "insurance_plan_code","base_rate"), with = FALSE]
    setnames(sco85,old = c("base_rate"),new = c("sco85"))

    # --- ECO-anchored SCO88 ---
    # Join ECO90 and SCO85 onto the AYP-anchored sco88 rows
    sco88 <- dplyr::full_join(sco88,eco90)
    sco88 <- sco88[!base_rate %in% c(0,NA,NaN,Inf,-Inf)]
    sco88 <- dplyr::full_join(sco88,sco85)
    sco88 <- sco88[!base_rate %in% c(0,NA,NaN,Inf,-Inf)]
    # Interpolate to 88% using ECO90/SCO85 anchors
    sco88[,base_rate_eco := base_rate + (eco90 - sco85)*((88-86)/(90-85))]

    # --- ECO-anchored SCO90 ---
    sco90 <- dplyr::full_join(sco90,eco90)
    # Keep only valid base_rate rows for subsequent calc (here the filter is negated vs sco88)
    sco90 <- sco90[!base_rate %in% c(0,NA,NaN,Inf,-Inf)]
    sco90 <- dplyr::full_join(sco90,sco85)
    sco90 <- sco90[!base_rate %in% c(0,NA,NaN,Inf,-Inf)]
    sco90[,base_rate_eco := base_rate + (eco90 - sco85)*((90-86)/(90-85))]

    rm(eco90,sco85);gc()
  }

  # --- Re-tag synthesized SCO rows with distinct plan codes ---
  # Convention: SCO88 = 31-33 + 10 to 41-43; SCO90 = 31-33 + 20 to 51-53
  sco88[,insurance_plan_code := insurance_plan_code + 10]
  sco90[,insurance_plan_code := insurance_plan_code + 20]

  # Average AYP-anchored and ECO-anchored base rates if both available
  # (produces a single 'base_rate' column per row)
  sco88[, base_rate := rowMeans(.SD, na.rm = TRUE),.SDcols = patterns("^base_rate_")]
  sco90[, base_rate := rowMeans(.SD, na.rm = TRUE),.SDcols = patterns("^base_rate_")]

  # Stack original SCO/ECO rows with the synthesized SCO88/SCO90 rows
  admfull <- data.table::rbindlist(list(admfull[insurance_plan_code %in% c(31:33,35:36,87:89)],sco88,sco90), fill = TRUE)

  rm(sco90,sco88);gc()

  # Drop any remaining rows with invalid base_rate values
  admfull <- admfull[!base_rate %in% c(0,NA,NaN,Inf,-Inf)]

  return(admfull)
}


