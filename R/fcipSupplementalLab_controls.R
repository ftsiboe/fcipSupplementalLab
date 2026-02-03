#' Create a control list of adjustment factors for FCIP-related packages
#'
#' @return A named list of control parameters, ready to be passed to other simulation functions.
#' @family helpers
#' @export
fcipSupplementalLab_controls <- function(
) {




  list(
    CALCPASS_TOKEN = Sys.getenv("CALCPASS_TOKEN", ""),
    CALIBRATE_TOKEN = Sys.getenv("CALIBRATE_TOKEN", ""),
    rfcipPRF_TOKEN = Sys.getenv("rfcipPRF_TOKEN", ""),
    rfcipEvaluator_TOKEN = Sys.getenv("rfcipEvaluator_TOKEN", ""),
    rfcipPreferences_TOKEN = Sys.getenv("rfcipPreferences_TOKEN", ""),
    rfcipReporter_TOKEN = Sys.getenv("rfcipReporter_TOKEN", ""),
    rfcipReSim_TOKEN = Sys.getenv("rfcipReSim_TOKEN", ""),
    rAgroClimate_TOKEN = Sys.getenv("rAgroClimate_TOKEN", "")
  )
}

