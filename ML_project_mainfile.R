## ---------------------------
##
## Script name: ML_project_mainfile
##
## Purpose of script: Main script
##
## Date Created: 2021-04-05
##
## ---------------------------

# Install and load packages
# Install and load packages
install.packages(c("tidyverse","foreign","lubridate","factoextra","dplyr","RPostgres","highfrequency","stringi",
                   "visreg","broom","RColorBrewer","magrittr", "psych", "data.table","Hmisc","filesstrings",
                   "stargazer", "tibble", "quantmod", "tidyquant", "matrixStats", "parallel","ggpubr"))
library(tidyverse)
library(lubridate)
library(foreign)
library(factoextra)
library(visreg)
library(broom)
library(RColorBrewer)
library(naniar)
library(stargazer)
library(tibble)
library(readr)
library(quantmod)
library(tidyquant)
library(matrixStats)
library(plyr)
library(dplyr)
library(magrittr)
library(readr)
library(psych)   
library(RPostgres)
library(highfrequency)
library(data.table)
library(Hmisc)
library(ggpubr)
library(data.table)
library(parallel)
library(filesstrings)


# Load all Library for CNN individually
devtools::install_github("rstudio/keras", dependencies = TRUE)
devtools::install_github("rstudio/tenserflow", dependencies = TRUE)
devtools::install_github("rstudio/reticulate")

library(keras)
library(tensorflow)
library(reticulate)

install_keras()
install_tensorflow()

# Cleaning environment
graphics.off()
rm(list = ls())
theme_set(theme_bw())

# Setting working directory to current active path from R script
current_path <- rstudioapi::getActiveDocumentContext()$path
setwd(dirname(current_path))

# No scientific notation
options(scipen = 999)
options(digits.secs=6)  

# Setting memory limit
memory.limit(size=560000)

# Call functions
source("ML_project_functions.R")

# Define Variables
user = 'markusfaessler'
password = 'X'
from = "20091231"
to = "20201231" 
tickers = "('V','JNJ', 'PG', 'MCD', 'DAL', 'HD', 'CVS', 'D', 'FL', 'DLB')"


######### Image Generation ######### 


# Get TAQ data and plot images
prices_all <- get_taq_data(user = user, password = password, 
                           from = from, to = to, tickers = tickers,
                           interval = interval)

#save(prices_all, file=".\\prices_all.Rdata")

# Get label returns (1 = positive return, 0 = negative return)
label_return <- label_sign(prices_all)

save(label_return, file=".\\label_return.Rdata")

# Move plots
move_plots_by_date(label_return, limit_date = "2013-12-14",
                   fig_path = ".\\images\\",
                   new_path = ".\\images_sorted\\")


######### Define Tensor Objects ######### 


# Define batch size and epochs
batch_size=256/2
epochs=20

# Get tensor objects
tensor_objects <- CNN_objects(train_image_files_path ="./images_sorted/Training/", 
                              test_image_files_path ="./images_sorted/Test/",
                              batch_size=batch_size, epochs=epochs, seed=123)

# Get all arrays
array_train <-  tensor_objects[[1]]
array_valid <-  tensor_objects[[2]]
array_test <-   tensor_objects[[3]]
array_full_l <- tensor_objects[[4]]

# Table of images per class
table(factor(array_test$classes))

# Number of training samples
train_samples <- array_train$n

# Number of training samples
valid_samples <- array_valid$n

# Number of test samples
test_samples <- array_full_l$n


######### CNN Training ######### 


# Setup CNN architecture
model <- CNN_model(filter=64, kernel_size=c(5,3), 
                   strides=c(1,3), pooling_size=c(2,1), 
                   learning_rate=0.00001, seed=1234)

# Train model
trained_model <- fit_cnn(model = model, 
                           train_generator = array_train, 
                           valid_generator = array_valid, 
                           step_epochs = as.integer(train_samples/batch_size),
                           nr_epochs = epochs,
                           valid_steps = as.integer(valid_samples/batch_size),
                           delta_improvement=0.0005,
                           waiting=3)


# Save model
save_model_hdf5(model, filepath = ".\\output_new2.h5")

save_model_weights_hdf5(model, filepath = '.\\output_weights_new2.h5')

# Load model
model <- load_model_hdf5(".\\output_new2.h5")
model %>% load_model_weights_hdf5(".\\output_new2.h5")

# Evaluate model on test data
model %>%
  evaluate(array_test, 
           steps = as.integer(array_test$n))


######### Prediction ######### 


# Predict classes on test data
out_of_sample_prediction <- predict_images(fitted_model = model,
                                           out_of_sample_array = array_train)

save(out_of_sample_prediction, file=".\\out_of_sample_prediction.Rdata")

# Predict in-sample
in_sample_prediction <- predict_images(fitted_model = model,
                                       out_of_sample_array = array_train)

save(in_sample_prediction, file=".\\in_sample_prediction.Rdata")

