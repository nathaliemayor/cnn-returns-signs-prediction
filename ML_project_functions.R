## ---------------------------
##
## Script name: ML_project_functions
##
## Purpose of script: Functions for main script
##
## Date Created: 2021-04-05
##
## ---------------------------

# Function which accesses, cleans and plots ticker data
get_taq_data <- function(user, password, from, to, tickers, interval){
  
  # Connect to WRDS
  wrds <- dbConnect(Postgres(),
                    host = 'wrds-pgdata.wharton.upenn.edu',
                    port = 9737,
                    dbname = 'wrds',
                    sslmode = 'require',
                    user = user,
                    password = password)
  
  
  # Get list of all available dates
  res <- dbSendQuery(wrds, "select distinct table_name
                     from information_schema.columns
                     where table_schema='taqmsec'
                     order by table_name")
  
  df_dates <- dbFetch(res, n = -1)
  dbClearResult(res)
  
  # Df_dates contains also quotes table names and indices
  dates_trades <- df_dates %>%
    filter(grepl("ctm",table_name), !grepl("ix_ctm",table_name)) %>%
    mutate(table_name = substr(table_name, 5, 12)) %>%
    unlist() 
  
  # Get Input dates
  input_d <- match(c(from, to), dates_trades)
  
  # Get all dates from and to input dates
  dd <- dates_trades[input_d[1]:input_d[2]]
  
  # Define progress bar
  pb <- winProgressBar(title="Progress Loop", min=0, label = "The Loop is currently running", max=length(dd), initial=0)
  stepi <- 0 
  
  # Create all lists for the loop
  prices_list <- list()
  
  # Fetch data from Wrds Via SQL loop
  for (i in dd){
    
    # Counting 
    stepi = stepi + 1
    
    #i = dd[1]
    # Define SQL statement which will access each daily table for all tickers 
    res <-
      dbSendQuery(wrds,
                  paste0(" select concat(date, ' ',time_m) as DT,",
                         " ex, sym_root, price, size, tr_corr, tr_scond",
                         " from taqmsec.ctm_", i,
                         " where (ex = 'N' or ex = 'T' or ex = 'Q' or ex = 'A')",
                         " and sym_root IN ", tickers,
                         " and price != 0 and tr_corr = '00'",
                         " and time_m between '09:30:00.00' and '16:00:00.00'"))
    
    # Jump over iteration if missing values in WRDS data bank 
    try({
      
      # Fetch data from SQL table and clean data
      dtaq <- dbFetch(res, n = -1) %>%
        # Transform as data table
        as.data.table() %>%
        # Set column dt as date format
        mutate(dt = lubridate::ymd_hms(dt, tz = "UTC")) %>%
        # Deletes all entries with abnormal trade conditions
        tradesCondition() %>%
        # Merges trade entries with same time stamp
        mergeTradesSameTimestamp()                            
      
      # Create all OHLCV charts
      ohlcv_data <- dtaq %>%
        # Calculate all OHLCV variables
        makeOHLCV(., alignBy = "minutes", alignPeriod = 5) %>%
        # Chart all OHLCV data and store it to defined path
        plot_OHLCV(., DAYS = 1, width_input = 4.25, heigth_input = 2.22, dpi_input = 72, PATH=".\\images\\")
      
      # Calculate daily prices of all symbols
      daily_prices <- dtaq %>%
        # Aggregate to daily prices
        aggregatePrice(., alignBy = "hours", alignPeriod = 24, marketOpen = "09:30:00", marketClose = "16:00:00", tz = "UTC", fill = TRUE) %>%
        # Select DT, Symbol and Price
        select(DT, SYMBOL, PRICE) %>%
        # Format prices for each symbol
        makeRMFormat() %>%
        # Delete duplicated rows
        unique(by = unique(.$SYMBOL))
      
      # Append daily prices to list
      prices_list[[i]] <- daily_prices
      
    }, silent=TRUE)
    
    # Print progress
    info <- sprintf("%d%% done", round((stepi/length(dd))*100))
    setWinProgressBar(pb, stepi, label=info)
    
    dbClearResult(res)
  }
  
  # Close Progress Window
  close(pb)
  
  
  # Unlist daily prices list and transform to data table
  prices_all <- as.data.table(Reduce(plyr::rbind.fill, prices_list)) %>%
    column_to_rownames(var="DT") %>%
    na.omit()
  
  # Adjust row names
  row.names(prices_all) <- substr(row.names(prices_all),1, 10)
  
  return(prices_all)
}

# Function which plots OHLCV charts
plot_OHLCV <- function(data, DAYS = 1, PATH, width_input, heigth_input, dpi_input){
  # Data prep
  # data = ohlcv_data
  # DAYS = 1
  dataset <- data %>% 
    # Group data by symbol
    group_by(SYMBOL) %>% 
    # Identify the day number for each SYMBOL
    mutate(GROUP=rleid(substr(DT, 1, 10)),
           # Add a grouping variable to identify the plots to do
           GROUP2=cut(GROUP, 
                      breaks = c(seq(from = 1, to = max(GROUP), by = DAYS), Inf),
                      include.lowest = T,
                      right = F)) %>% 
    # Re-group by SYMBOL and the previous grouping variable
    group_by(SYMBOL, GROUP2) %>% 
    # Dropping unnecessary days
    mutate(GROUP2=ifelse(length(levels(GROUP2))=="1", 1, ifelse(length(unique(GROUP))!=DAYS, NA, GROUP2))) %>% 
    na.omit() %>% 
    group_by(SYMBOL, GROUP2) %>% 
    # Creating the labels
    mutate(LABEL=paste(SYMBOL, gsub("-", "", substr(DT, 1, 10)), sep = "_")) %>%
    group_by(SYMBOL, GROUP2) %>% 
    mutate(LABEL=last(LABEL)) %>% 
    ungroup %>% 
    # Removing unnecessary variables
    select(-c(GROUP, GROUP2)) %>% 
    mutate(DT=as.character(1:nrow(.)))
  
  ## Making the plots
  # Split the data by LABEL
  dataset <- dataset %>% 
    split(., .$LABEL)
  
  # Define cluster number for parallel programming
  clp <- makeCluster(detectCores(logical=FALSE), type = "SOCK", useXDR=FALSE)
  
  # Load necessary libraries to each worker
  clusterEvalQ(cl=clp, {
    library(dplyr)
    library(lubridate)
    library(tidyquant)
    library(ggpubr)
    library(data.table)
    library(ggplot2)
    library(ggpubr)
    library(parallel)
  })
  # Begin parallel process
  parLapply(cl=clp, X=dataset, function(DATA){
    
    # Plotting OHLC chart
    plot1 <- DATA %>%
      ggplot(., aes(x=DT))+
      geom_barchart(aes(open = OPEN, high = HIGH, low = LOW, close = CLOSE),
                    colour_up = "white",
                    colour_down = "white",
                    fill_up = "white",
                    fill_down = "white",
                    size=0.5/DAYS)+
      theme_void()+
      theme(plot.background = element_rect(fill = "black"),
            plot.margin = unit(c(-0.05, -0, -0.1, -0.07), "cm"))
    
    # Plotting Volume barchart
    plot2 <- ggplot(DATA, aes(x=DT, y=VOLUME))+
      geom_segment(aes(xend=DT, yend=0), size = 0.5/DAYS, color="white")+
      theme_void()+
      theme(plot.background = element_rect(fill = "black"),
            plot.margin = unit(c(-0.05, -0, -0.1, -0.07), "cm"))
    # Merge both plots
    ggarrange(plot1,
              plot2,
              ncol = 1, nrow = 2,
              heights = c(5, 3),
              widths = c(1, 0.05, 1))
    # Save image in directory
    ggsave(paste(PATH, unique(DATA$LABEL), ".png", sep=""), scale = DAYS, width = width_input, height = heigth_input, limitsize = FALSE, dpi = dpi_input) #Think about dpi! 36 
  })
  # Finish parallel process
  stopCluster(clp)
  
} 

# Function which creates return labels of prices
label_sign <- function(data){
  label <- data %>%
    # If difference from t to t+1 is positive, then set value 1, else 0
    mutate_all(~c(first(.), diff(., lag = 1))) %>%
    # Lead all variables
    dplyr::mutate_all(., lead) %>%
    # Get signs of returns
    sign() %>%
    # Change if return positive set to 1, else 0 
    mutate_all(~replace(., .<=-1, 0)) %>%
    # Omit NAs
    na.omit() 
  
  # Return label
  return(label)
}

# Function which sorts plots according to label input 
move_plots_by_date <- function(label_data, limit_date, fig_path, new_path){
  # With labeled data
  new_dir <- label_data %>% 
    # save row names as a new variable
    mutate(DT=row.names(.)) %>% 
    # gather SYBOLS and labels
    gather(SYMBOL, FOLDER, -DT) %>% 
    # Create the same plot labels
    mutate(LABEL=paste(paste(SYMBOL, gsub("-", "", DT), sep="_"), ".png", sep=""),
           TRAIN_TEST=ifelse(ymd(DT)<=ymd(limit_date), "Training", "Test")) %>%
    #Select only necessary variables
    select(LABEL, FOLDER, TRAIN_TEST)
  # Save de plot names
  PLOTS <- list.files(fig_path)
  
  # Create the folders for test and training (in there each classification)
  c(new_path, 
    paste(new_path, c("Training", "Test"), "/", sep=""), 
    paste(new_path, "Training/", unique(new_dir$FOLDER), "/", sep=""),
    paste(new_path, "Test/", unique(new_dir$FOLDER), "/", sep="")) %>% 
    sapply(dir.create)
  
  # Check which files are in the labeled data
  new_dir <- new_dir %>%
    filter(LABEL %in% PLOTS) %>%
    # Create the variable FROM, to find the plot
    mutate(FROM=paste(fig_path, LABEL, sep=""),
           # Create the variable TO, to set where the plot will be move
           TO=paste(new_path, TRAIN_TEST, "/", FOLDER, "/", sep=""))
  
  # Moving the plots
  mapply(function(from, to){
    file.move(from, to)
  }, from=new_dir$FROM, to=new_dir$TO)
  
  
}

# Define train and test arrays for CNN
CNN_objects <- function(train_image_files_path, test_image_files_path, img_width=306, img_height=159, 
                        batch_size=128, epochs=20, seed=123){ 
  # Train Image data augmentation
  train_data_gen <- image_data_generator(
    rescale = 1/255,
    validation_split = 0.3
  )
  
  # Test image data augmentation
  test_data_gen <- image_data_generator(
    rescale = 1/255
  )  
  
  # Set classes
  classes_returns <- c("0", "1")
  
  # Number of output classes 
  output_n <- length(classes_returns)
  
  # Image size to scale Width X Height (306x159)
  target_size <- c(img_width, img_height)
  
  # Setting RGB channels as images are gray-scale
  channels <- 1
  
  # Training Images
  train_image_array_gen <- flow_images_from_directory(train_image_files_path, 
                                                      train_data_gen,
                                                      target_size = target_size,
                                                      color_mode = "grayscale",
                                                      class_mode = "binary",
                                                      classes = classes_returns,
                                                      batch_size = batch_size,
                                                      seed = seed,
                                                      subset = "training")
  
  # Validation Images
  valid_image_array_gen <- flow_images_from_directory(train_image_files_path, 
                                                      train_data_gen,
                                                      target_size = target_size,
                                                      color_mode = "grayscale",
                                                      class_mode = "binary",
                                                      classes = classes_returns,
                                                      batch_size = batch_size,
                                                      seed = seed,
                                                      subset = "validation")
  
  # In-Sample images (train + validation)
  in_sample_image_array_gen <- flow_images_from_directory(train_image_files_path, 
                                                          train_data_gen,
                                                          target_size = target_size,
                                                          color_mode = "grayscale",
                                                          class_mode = "binary",
                                                          classes = classes_returns,
                                                          batch_size = batch_size,
                                                          seed = seed,
                                                          shuffle = FALSE)
  
  # Testing images
  test_image_array_gen <- flow_images_from_directory(test_image_files_path, 
                                                     test_data_gen,
                                                     target_size = target_size,
                                                     color_mode = "grayscale",
                                                     class_mode = "binary",
                                                     batch_size = 1,              # Think about this
                                                     classes = classes_returns,
                                                     seed = 42,
                                                     shuffle = FALSE)
  
  return(list(train_image_array_gen, valid_image_array_gen, in_sample_image_array_gen, test_image_array_gen))
  
}

# Function which sets up model architecture
CNN_model <- function(img_width=306, img_height=159, filter=64, kernel_size=c(5,3), strides=c(1,3), pooling_size=c(2,1), 
                      learning_rate=0.00005, seed=123){
  
  # CNN Setup 
  model <- keras_model_sequential()
  
  # Adding layers
  model %>%
    layer_conv_2d(filter = filter, kernel_size = kernel_size, strides = strides, input_shape = c(img_width, img_height, 1),
                  kernel_initializer = "glorot_uniform", bias_initializer =  initializer_glorot_uniform(seed = seed)) %>%
    layer_batch_normalization() %>%
    layer_activation_leaky_relu() %>%
    
    # Use max pooling
    layer_max_pooling_2d(pool_size = pooling_size) %>%
    
    
    # Second hidden layer
    layer_conv_2d(filter = filter*2, kernel_size = kernel_size, strides = strides, kernel_initializer = "glorot_uniform",
                  bias_initializer =  initializer_glorot_uniform(seed = seed)) %>%
    layer_batch_normalization() %>%
    layer_activation_leaky_relu() %>%
    
    # Use max pooling
    layer_max_pooling_2d(pool_size = pooling_size) %>%
    
    
    # Third hidden layer
    layer_conv_2d(filter = filter*4, kernel_size = kernel_size, strides = strides, kernel_initializer = "glorot_uniform", 
                  bias_initializer =  initializer_glorot_uniform(seed = seed)) %>%
    layer_batch_normalization() %>%
    layer_activation_leaky_relu() %>%
    
    # Use max pooling
    layer_max_pooling_2d(pool_size = pooling_size) %>%
    
    
    # Fourth hidden layer
    layer_conv_2d(filter = filter*8, kernel_size = kernel_size, strides = strides, kernel_initializer = "glorot_uniform",
                  bias_initializer =  initializer_glorot_uniform(seed = seed)) %>%
    layer_batch_normalization() %>%
    layer_activation_leaky_relu() %>%
    
    # Use max pooling
    layer_max_pooling_2d(pool_size = pooling_size) %>%
    
    
    # fifth hidden layer
    layer_conv_2d(filter = filter*16, kernel_size = kernel_size, strides = strides, kernel_initializer = "glorot_uniform", padding="same",
                  bias_initializer =  initializer_glorot_uniform(seed = seed)) %>%
    layer_batch_normalization() %>%
    layer_activation_leaky_relu() %>%
    
    # Use max pooling
    layer_max_pooling_2d(pool_size = pooling_size) %>%
    
    
    # Flatten max filtered output into feature vector 
    # And feed into dense layer which are projected onto output layer
    layer_flatten() %>%
    layer_dense(2) %>%
    #layer_dropout(layer_dropout)
    layer_activation_softmax()
  
  # Compile
  model %>% compile(
    loss = "binary_crossentropy",
    optimizer = optimizer_adam(lr = learning_rate), 
    metrics = "accuracy" 
  )
  
  # Count number of parameters of model
  print(count_params(model))
  
  # Model summary
  print(model)
  
  # Return model
  return(model)
}

# Function which trains the model 
fit_cnn <- function(model, train_generator, valid_generator, step_epochs, nr_epochs, valid_steps, delta_improvement=0.0001, waiting=1){
  
  # Train model
  model_trained <- model %>% fit(
    
    # Train data Input
    train_generator,
    
    # Epochs
    steps_per_epoch = step_epochs, 
    epochs = nr_epochs, 
    
    # Validation data
    validation_data = valid_generator,
    validation_steps = valid_steps,
    
    # Print progress
    verbose = 2,
    
    # Stop training conditions
    callbacks = callback_early_stopping(
      monitor = "val_loss",
      min_delta = delta_improvement, 
      patience = waiting,
      verbose = 1,
      mode = "auto"
    )
  )
}

# Function which assigns prediction probability of CNN
predict_images <- function(fitted_model, image_array){
  # Reset array
  image_array$reset()
  
  # Calculate prediction
  pred <- fitted_model %>% predict(image_array, steps = as.integer(image_array$n))
  
  # Clean predictions and set column names
  pred_clean <- pred %>%
    round(digits = 4) %>%
    as.data.table() %>%
    set_colnames(c("Likelihood_0", "Likelihood_1"))
  
  # Break down file names to label, symbol and date
  break_filenames <- as.data.table(image_array$filenames) %>%
    separate(V1, c("Label", "Symbol", "DT")) 
  
  # Change date format
  break_filenames$DT <- as.Date.character(break_filenames$DT, "%Y%m%d")
  
  # Combine time series prediction of CNN and image names
  final_prediction <- cbind(pred_clean, break_filenames)
  
  # Return prediction
  return(final_prediction)
}