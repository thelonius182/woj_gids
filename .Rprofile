check_git_branch <- function() {
  
  if (file.exists(".git")) {
    
    branch <- system("git rev-parse --abbrev-ref HEAD", intern = TRUE)
    
    if (branch == "main") {
      message(">>>\n>>>    WARNING: you are on the MAIN BRANCH, normally used for merging only.\n>>>\n")
    }
  }
}

message(sprintf("using .Rprofile in %s", getwd()))
check_git_branch()
