# Hard reset of workspace
rm(list = ls(all = TRUE)); gc()

# Clean generated artifacts
unlink(c(
  "NAMESPACE",
  #list.files("./data", full.names = TRUE),
  list.files("./man",  full.names = TRUE)
))


if(toupper(as.character(Sys.info()[["sysname"]])) %in% "WINDOWS"){
  source( file.path(dirname(dirname(getwd())),"codeLibrary.R"))
  list_function <- c(
    file.path(codeLibrary,"miscellaneous/build_internal_datasets.R"),
    file.path(codeLibrary,"github_tools/install_from_private_repo.R"),
    file.path(codeLibrary,"plot/ers_theme.R"),
    paste0(file.path(codeLibrary,"fcip//"),
           c("get_fcip_agents.R",
             "clean_supplemental_plan_shares.R",
             "clean_rma_sobtpu.R",
             "build_supplemental_offering_and_adoption.R"))
  )
  file.copy(from= list_function, to = "R/", overwrite = TRUE, recursive = FALSE, copy.mode = TRUE)
}


for(i in c("setup_environment")){
  download.file(
    paste0("https://raw.githubusercontent.com/ftsiboe/USFarmSafetyNetLab/refs/heads/main/R/",i,".R"),
    paste0("./R/",i,".R"), mode = "wb", quiet = TRUE)
}

# Sanity pass through R/ sources: shows any non-ASCII characters per file
for (i in list.files("R", full.names = TRUE)) {
  print(paste0("********************", i, "********************"))
  tools::showNonASCIIfile(i)
}

# Rebuild documentation from roxygen comments
devtools::document()

# Check man pages only (faster than full devtools::check)
devtools::check_man()

# Build PDF manual into the current working directory
devtools::build_manual(path = getwd())

# Optional: run tests / full package check (uncomment when needed)
# devtools::test()
devtools::check()
