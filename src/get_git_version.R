pacman::p_load(git2r)

# Get the repository
# repo <- repository("C:/Users/nipper/r_projects/released/woj_gids")
# repo <- repository("C:/Users/nipper/r_projects/woj_gids")
# repo <- repository("g:/R_released_projects/woj_gids")
repo <- repository("C:/cz_salsa/r_proj_develop/cz_gidsweek")
# Get the current branch name
branch <- repository_head(repo)$name

# Get the latest commit
latest_commit <- commits(repo, n = 1)[[1]]

# Get the date of the latest commit
commit_date <- latest_commit$author$when

message(paste0("git branch: ", branch, ", dd ", commit_date))
message(paste0("local fs: ", repo$path))

gv <- salsa_git_version("C:/Users/nipper/r_projects/released/woj_gids")
gv <- salsa_git_version("g:/R_released_projects/woj_gids")
gv <- salsa_git_version("C:/Users/nipper/r_projects/woj_gids")
gv <- salsa_git_version("C:/cz_salsa/r_proj_develop/cz_gidsweek")
