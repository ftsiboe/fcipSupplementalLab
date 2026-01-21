#' Download all assets from a GitHub release with rate limiting
#'
#' Downloads all files attached to a specified GitHub release tag while
#' **throttling requests** to avoid GitHub rate limits and abuse protection.
#' This helper is designed for releases containing many or large assets
#' (e.g., `.rds` outputs generated on HPC systems).
#'
#' The function downloads assets incrementally, pauses between requests,
#' and retries failed downloads across multiple rounds. Already-downloaded
#' files are skipped, allowing the function to safely resume after
#' interruptions or rate-limit errors.
#'
#' @details
#' The function:
#' \enumerate{
#'   \item Constructs a default output directory
#'         (\code{data-raw/releases/{release_tag}}) if none is supplied.
#'   \item Queries GitHub once to obtain the list of release assets.
#'   \item Downloads assets **one at a time** using \pkg{piggyback}.
#'   \item Pauses for \code{sleep_seconds} between downloads to reduce
#'         request bursts.
#'   \item Retries failed or missing downloads for up to \code{max_rounds}.
#'   \item Skips files that already exist locally.
#' }
#'
#' This approach is especially useful when GitHub returns repeated
#' \code{HTTP 403 (Forbidden)} errors during bulk downloads.
#'
#' Authentication via a GitHub personal access token (PAT) is strongly
#' recommended, even for public repositories.
#'
#' @param owner Character string giving the GitHub repository owner
#'   (e.g., \code{"ftsiboe"}).
#' @param repository Character string giving the GitHub repository name
#'   (e.g., \code{"indexDesignWindows"}).
#' @param release_tag Character string specifying the GitHub release tag
#'   whose assets should be downloaded.
#' @param output_directory Optional character string specifying the local
#'   directory where release assets should be saved. Defaults to
#'   \code{data-raw/releases/{release_tag}}.
#' @param github_token Optional GitHub personal access token (PAT).
#'   Passed to \pkg{piggyback} via \code{.token}. Strongly recommended.
#' @param sleep_seconds Numeric scalar giving the number of seconds to pause
#'   between individual file downloads. Increasing this value reduces the
#'   likelihood of triggering GitHub rate limits.
#' @param max_rounds Integer giving the maximum number of retry rounds.
#'   Each round attempts to download any files still missing locally.
#'
#' @return
#' Invisibly returns \code{NULL}. Files are downloaded for their side effects.
#' @export
get_study_releases <- function(
    owner,
    repository,
    release_tag,
    output_directory = NULL,
    github_token = NULL,
    sleep_seconds = 3,      
    max_rounds    = 3       
){
  
  if (is.null(output_directory) || !nzchar(output_directory)) {
    output_directory <- file.path("data-raw", "releases", release_tag)
  }
  if (!dir.exists(output_directory)) dir.create(output_directory, recursive = TRUE)
  
  repo <- paste(owner, repository, sep = "/")
  
  # Get asset list once, then download one-by-one with pacing
  rel <- piggyback::pb_list(repo = repo, tag = release_tag, .token = github_token)
  
  if (NROW(rel) == 0) {
    stop("No assets found for tag: ", release_tag, call. = FALSE)
  }
  
  files <- rel$file_name
  
  for (round in seq_len(max_rounds)) {
    remaining <- files[!file.exists(file.path(output_directory, files))]
    if (length(remaining) == 0) break
    
    message("Round ", round, "/", max_rounds, ": downloading ", length(remaining), " files...")
    
    for (f in remaining) {
      piggyback::pb_download(
        file      = f,
        repo      = repo,
        tag       = release_tag,
        dest      = output_directory,
        overwrite = TRUE,
        .token    = github_token
      )
      Sys.sleep(sleep_seconds)
    }
  }
  
  invisible(NULL)
}






