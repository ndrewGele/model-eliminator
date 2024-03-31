# This script checks performance of existing models,
# and keeps only the top performers.

library(dplyr)
library(dbplyr)


# Connect to Data Sources -------------------------------------------------

# Initialize S3 Connection and Bucket Info
s3 <- paws.storage::s3(
  config = list(
    credentials = list(anonymous = TRUE),
    endpoint = glue::glue(
      'http://',
      Sys.getenv('SEAWEED_HOST'),
      ':',
      Sys.getenv('SEAWEED_S3_PORT')
    ),
    region = 'xx' # can't be blank
  )
)

bucket_names <- s3$list_buckets() %>%
  purrr::pluck('Buckets') %>%
  purrr::map_chr(purrr::pluck, 'Name')

bucket_exists <- Sys.getenv('SEAWEED_MODEL_BUCKET') %in% bucket_names

# Connect to DB
db_con <- DBI::dbConnect(
  drv = RPostgres::Postgres(),
  dbname = Sys.getenv('POSTGRES_DB'),
  host = Sys.getenv('POSTGRES_HOST'),
  port = Sys.getenv('POSTGRES_PORT'),
  user = Sys.getenv('POSTGRES_USER'),
  password = Sys.getenv('POSTGRES_PASSWORD')
)

table_exists <- DBI::dbExistsTable(db_con, 'models')


# Handle missing prereqs --------------------------------------------------

if (!bucket_exists) {
  message('Model bucket does not exist.')
  
  if (table_exists) {
    message('...but database table does exist. ',
            'Dropping table.')
    
    DBI::dbRemoveTable(db_con, 'models')
    
    message('Deleted models table.')
  }
  
  message('Sleeping for an hour before restarting.')
  DBI::dbDisconnect(db_con)
  Sys.sleep(60 * 60)
  stop('Done sleeping. Stopping process.')
}

if (!table_exists) {
  message('Models table does not exist.')
  
  if (bucket_exists) {
    message('...but Seaweed bucket does exist. ',
            'Emptying bucket.')
    
    s3$delete_bucket(Bucket = Sys.getenv('SEAWEED_MODEL_BUCKET'))
    
    message(glue::glue('Deleted Seaweed Bucket: {Sys.getenv("SEAWEED_MODEL_BUCKET")}.'))
  }
  
  message('Sleeping for an hour before restarting.')
  DBI::dbDisconnect(db_con)
  Sys.sleep(60 * 60)
  stop('Done sleeping. Stopping process.')
}


# Pull Data to Manage -----------------------------------------------------

# Get data for models that haven't been retired
check_for_updates_df <- db_con %>%
  tbl('models') %>%
  inner_join(
    db_con %>%
      tbl('models') %>%
      group_by(file_name) %>%
      summarise(update_timestamp = max(update_timestamp, na.rm = TRUE)),
    by = c(
      'file_name',
      'update_timestamp'
    )
  ) %>%
  filter(status %in% c('new', 'champion')) %>%
  collect()


# Check Bucket and DB for Parity ------------------------------------------

models_in_db <- check_for_updates_df$file_name
models_in_bucket <- s3$list_objects(Bucket = Sys.getenv('SEAWEED_MODEL_BUCKET')) %>% 
  purrr::pluck('Contents') %>% 
  purrr::map_chr(purrr::pluck, 'Key')

missing_from_db <- models_in_bucket[!models_in_bucket %in% models_in_db]
missing_from_bucket <- models_in_db[!models_in_db %in% models_in_bucket]

if(length(missing_from_db) > 0) {
  
  objects_to_delete <- purrr::map(
    .x = missing_from_db,
    .f = \(x) list(Key = x)
  )
  
  res <- s3$delete_objects(
    Bucket = Sys.getenv('SEAWEED_MODEL_BUCKET'),
    Delete = list(
      Objects = objects_to_delete
    )
  )
  
  message(glue::glue('Deleted {length(res$Deleted)} objects not found in DB.'))
}

if(length(missing_from_bucket) > 0) {
  to_missing_df <- check_for_updates_df %>% 
    filter(file_name %in% missing_from_bucket) %>% 
    mutate(status = 'missing')
  
  check_for_updates_df <- anti_join(
    check_for_updates_df,
    to_missing_df,
    by = 'file_name'
  ) # will bind_rows this before db write
  
  message(glue::glue(
    '{nrow(to_missing_df)} objects not found in bucket to ',
    'be updated in DB write shortly.'
  ))
}


# Perform Model Lifecycle Updates -----------------------------------------

# Determine champions, update status and timetstamp if necessary
lifecycle_timestamp <- Sys.time()
to_champion_df <- check_for_updates_df %>% 
  group_by(name) %>% 
  slice_min(order_by = holdout_perf, n = 1) %>% 
  slice_head(n = 1) %>% # dedupe arbitrarily if performance tie
  mutate(
    update_timestamp = case_when(
      status == 'new' ~ lifecycle_timestamp,
      TRUE ~ update_timestamp
    ),
    status = case_when(
      update_timestamp == lifecycle_timestamp ~ 'champion',
      TRUE ~ status
    )
  )

# Retire everything else
to_retire_df <- check_for_updates_df %>%
  anti_join(
    to_champion_df, 
    by = 'hash'
  ) %>% 
  mutate(
    status = 'retired',
    update_timestamp = lifecycle_timestamp
  )

update_df <- bind_rows(
  to_champion_df, 
  to_retire_df
)

# Include missing models from before, if there were any
if(length(missing_from_bucket) > 0) {
  update_df <- bind_rows(
    update_df,
    to_missing_df %>% 
      mutate(update_timestamp = lifecycle_timestamp)
  )
}


# Erase files from Bucket -------------------------------------------------

if(nrow(to_retire_df) > 0) {
  objects_to_delete <- purrr::map(
    .x = filter(to_retire_df, status == 'retired')$file_name,
    .f = \(x) list(Key = x) # this dictionary-like format is required
  )
  
  res <- s3$delete_objects(
    Bucket = Sys.getenv('SEAWEED_MODEL_BUCKET'),
    Delete = list(
      Objects = objects_to_delete
    )
  )
  
  message(glue::glue('Deleted {length(res$Deleted)} objects.'))
} else {
  message('No models to retire.')
}


# Write to database -------------------------------------------------------

# Check for duplicate data before writing to table
existing_df <- db_con %>% 
  tbl('models') %>% 
  select(name, hash, update_timestamp) %>% 
  collect()

pre_anti <- nrow(update_df)

update_df <- anti_join(
  update_df,
  existing_df,
  by = c('name', 'hash', 'update_timestamp')
)

post_anti <- nrow(update_df)

if(pre_anti != post_anti) {
  message(glue::glue(
    'Removed {pre_anti - post_anti} rows before writing to DB.'
  ))
}

if(nrow(update_df) > 0) {
  update_df %>%
    DBI::dbAppendTable(
      conn = db_con,
      name = 'models',
      value = .
    )
}

# Log before sleeping and looping again
message('Wrote ', nrow(update_df), ' new records to models table. ',
        'Sleeping for 1 hour.')
DBI::dbDisconnect(db_con)
Sys.sleep(60 * 60 * 1)
stop('Done sleeping. Stopping process.')
