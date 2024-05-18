# Load necessary packages
pacman::p_load(jsonlite, httr, stringr, yaml, gmailr, fs)

config <- read_yaml("config.yaml")

qfns <- dir_ls(path = "C:/cz_salsa/gidsweek_uploaden/", 
               type = "file",
               regexp = "WJ_gidsweek_\\d{4}.*[.]json$") |> sort(decreasing = T)

json_file <- qfns[1]
json_data <- fromJSON(json_file)
keys <- names(json_data)
new_import_hash <- UUIDgenerate(use.time = T, output = "string")

for (k1 in keys) {
  query_string <- list_to_query(json_data[[k1]])
  url_with_query <- paste0(config$wp_endpoint, 
                           "?action=import-program-item&", 
                           query_string,
                           "&import-hash=",
                           new_import_hash)
  
  # Send the request
  response <- GET(url_with_query)
  
  # Check the response
  status_code <- status_code(response)
  response_content <- content(response, "text") |> str_sub(-7)
  
  # Output the response status and content
  cat("Status Code:", status_code, "\n")
  cat("Response Content:", str_sub(response_content, -7), "\n")
  cat("Response Content:", response_content, "\n")
  str_extract(response_content, ".*\\<p>(.*)\\<\\p>.*", group = 1)
}
