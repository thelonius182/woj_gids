# Load necessary packages
pacman::p_load(jsonlite, httr)

# Step 1: Read the JSON file
json_file <- "C:/cz_salsa/gidsweek_uploaden/wj_wppost_uploadtest.json"
json_data <- fromJSON(json_file)

# Use URLencode with reserved = TRUE to ensure all special characters are encoded
url_encode_param <- function(value) {
  # URL encode the parameter, handling spaces and special characters
  URLencode(as.character(value), reserved = TRUE)
}

# Function to convert a list to a query string with proper URL encoding
list_to_query <- function(lst) {
  params <- sapply(names(lst), function(key) {
    # URL encode both the key and value
    key_encoded = url_encode_param(key) 
    value_encoded = url_encode_param(lst[[key]])
    paste0(key_encoded, "=", value_encoded)
  })
  # Join parameters with '&' to form the query string
  paste(params, collapse = "&")
}

# Create query string from JSON data
query_string <- list_to_query(json_data)

# Step 3: Send the GET request to the endpoint with the query parameters
endpoint <- "https://wpdev3.concertzender.nl/upload-program-item-endpoint.php"

# Create the full URL with the query string
url_with_query <- paste0(endpoint, "?", query_string)

# Send the GET request
response <- GET(url_with_query)

# Check the response
status_code <- status_code(response)
response_content <- content(response, "text")

# Output the response status and content
cat("Status Code:", status_code, "\n")
cat("Response Content:", response_content, "\n")
