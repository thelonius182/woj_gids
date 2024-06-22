message(sprintf("using .Rprofile in %s", getwd()))

get_git_branch <- function() {
  system("git rev-parse --abbrev-ref HEAD", intern = TRUE)
}

set_config <- function() {
  
  if (get_git_branch() == "main") {
    source("src/config_prd.R")
  } else {
    source("src/config_dev.R")
  }
}

check_git_branch <- function() {
  
    if (get_git_branch() == "main") {
      message(">>>\n>>>    WARNING: you are on the MAIN BRANCH, normally used for merging only.\n>>>\n")
    }
}

set_config()
check_git_branch()
