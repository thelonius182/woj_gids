# add/refresh ML-tracklists in the WoJ-programme guide

# Path to R executable
$RPath = "C:\Program Files\R\R-4.4.0\bin\x64\Rscript.exe"

# Path to R script
$RScriptPath = "C:\Users\nipper\r_projects\released\woj_gids\src\add_ml_tracklists_to_wp.R"

cd "C:\Users\nipper\r_projects\released\woj_gids\"

# Run the R script
& $RPath $RScriptPath
