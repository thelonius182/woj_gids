# add/refresh ML-tracklists in the WoJ-programme guide

# Path to R executable
$RPath = "C:\Program Files\R\R-4.4.1\bin\x64\Rscript.exe"

# Path to R script
$RScriptPath = "g:\R_released_projects\woj_gids\src\add_ml_tracklists_to_wp.R"

# Run the R script
cd g:
cd .\R_released_projects\woj_gids\
& $RPath $RScriptPath
