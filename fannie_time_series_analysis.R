# MORTGAGE LOAN ANALYSIS - 65M ROWS / 7GB SQL DB

library(tidyverse)
library(lubridate)
library(tidyquant)
library(plotly)
library(tictoc)

library(dbplyr)

library(DBI)
library(RSQLite)
library(connections) # remotes::install_github("edgararuiz/connections")

# 1.0 CONNECT TO TABLES ----

con_fannie <- connection_open(drv = SQLite(), dbname = "fannie_mae_loans.sqlite")

acquisitions_query <- tbl(con_fannie, "acquisitions")
acquisitions_query %>% glimpse()

performance_query <- tbl(con_fannie, "performance")
performance_query %>% glimpse()

tic()
performance_query %>% count()
toc()

# 2.0 EDA - Inspecting Loan Delinquency ----


loan_delinquency_query <- performance_query %>%
    head(10000) %>%
    filter(current_loan_delinquency_status > 0.5) 

loan_delinquency_query %>% show_query()

loan_ids_delinquent <- loan_delinquency_query %>%
    distinct(loan_id) %>%
    pull(loan_id)

loan_id <- loan_ids_delinquent[1]

plot_upb <- function(loan_ID) {
    
    g <- tbl(con_fannie, "performance") %>%
        # head(10000) %>%
        filter(loan_id == loan_ID) %>%
        mutate(current_upb = ifelse(is.na(current_upb), 0, current_upb)) %>%
        collect() %>%
        
        select(monthly_reporting_period, current_upb, current_loan_delinquency_status) %>%
        mutate(monthly_reporting_period = ymd(monthly_reporting_period)) %>%
        # mutate(delinquent = ifelse(current_loan_delinquency_status == 1, "Yes", "No")) %>%
        
        ggplot(aes(monthly_reporting_period, current_upb, color = current_loan_delinquency_status)) +
        geom_point() +
        expand_limits(y = 0) +
        scale_color_viridis_c() +
        theme_tq()
    
    ggplotly(g)
}

plot_delinquency <- function(loan_ID) {
    
    g <- tbl(con_fannie, "performance") %>%
        # head(10000) %>%
        filter(loan_id == loan_ID) %>%
        mutate(current_upb = ifelse(is.na(current_upb), 0, current_upb)) %>%
        collect() %>%
        
        select(monthly_reporting_period, current_loan_delinquency_status) %>%
        mutate(monthly_reporting_period = ymd(monthly_reporting_period)) %>%
        # mutate(delinquent = ifelse(current_loan_delinquency_status == 1, "Yes", "No")) %>%
        
        ggplot(aes(monthly_reporting_period, current_loan_delinquency_status, 
                   group = loan_id)) +
        geom_line() +
        geom_point() +
        expand_limits(y = 0) +
        scale_color_viridis_c() +
        theme_tq()
    
    ggplotly(g)
}


# 2.1 Good Customer: Almost Perfect Repayment History ----
plot_upb(loan_ID = loan_ids_delinquent[2])
plot_delinquency(loan_ids_delinquent[2])

# 2.2 OK Customer: Falls Behind But Able to Manage ----
plot_upb(loan_ID = "100670358700")
plot_delinquency(loan_ids_delinquent[15])

# 2.3 Bad Customer: Probably Never Intended to Pay ----
plot_upb("138315939282")
plot_delinquency("138315939282")


# 3.0 ROLLING WINDOW FUNCTIONS ----

rolling_delinquency_query <- tbl(con_fannie, "performance") %>%
    select(loan_id, monthly_reporting_period, current_loan_delinquency_status) %>%
    
    group_by(loan_id) %>%
    window_frame(from = -3, to = 0) %>%
    window_order(monthly_reporting_period) %>%
    mutate(rolling_mean_3 = mean(current_loan_delinquency_status, na.rm = TRUE)) %>%
    ungroup()

rolling_delinquency_query %>% show_query()

rolling_delinquency_query %>%
    filter(rolling_mean_3 >= 3) %>%
    show_query()

# Takes 2-minutes
tic()
rolling_delinquency_tbl <- rolling_delinquency_query %>%
    filter(rolling_mean_3 >= 3) %>%
    collect()
toc()

rolling_delinquency_tbl %>%
    group_by(loan_id) %>%
    summarize(max_delinquency = max(rolling_mean_3)) %>%
    ungroup() %>%
    arrange(desc(max_delinquency))

tbl(con_fannie, "performance") %>%
    filter(loan_id == "138315939282") %>%
    collect() %>%
    View()

plot_upb(loan_ID = "108837440379")
plot_delinquency(loan_ID = "108837440379")

# 4.0 LL PRO BONUS ----
# - Incorprate Target Variable into acquisitions
# - Analysis on Which Features Contribute

# 4.1 Target Encoding ----
bad_loans_tbl <- rolling_delinquency_tbl %>%
    distinct(loan_id) %>%
    add_column(target = 1)

# Takes 1-minute
all_loans_tbl <- performance_query %>%
    distinct(loan_id) %>%
    collect() %>%
    
    # Join with bad loans (adds target column) & replace NA with 0
    left_join(bad_loans_tbl) %>%
    mutate(target = ifelse(is.na(target), 0, target))
    
# 4.2 Join Targets with acquisition data ----

acquisitions_tbl <- acquisitions_query %>% collect()

training_data_tbl <- all_loans_tbl %>%
    left_join(acquisitions_tbl)

# 4.3 EDA ----

library(DataExplorer)
library(correlationfunnel)
library(recipes)

training_data_tbl %>% glimpse()

plot_missing(training_data_tbl)
profile_missing(training_data_tbl) %>% arrange(desc(pct_missing))

names_to_remove <- c(
    # Missing > 10%
    "original_coborrower_credit_score", 
    "mortgage_insurance_type", 
    "primary_mortgage_insurance_percent",
    
    # ID Fields
    "loan_id",
    "file",
    
    # Date Fields
    "original_date",
    "first_pay_date"
    )

# 4.4 Clean Data Set ----
# recipes is covered at great length in DS4B 201-R. 
# Takes 1-minute
recipe_obj <- recipe(target ~ ., data = training_data_tbl) %>%
    step_rm(names_to_remove) %>%
    step_medianimpute(all_numeric()) %>%
    step_modeimpute(all_nominal()) %>%
    step_nzv(all_predictors()) %>%
    prep()

train_clean_tbl <- bake(recipe_obj, new_data = training_data_tbl)

train_clean_tbl %>% glimpse()

# 4.5 Mine Relationships ----
# Use correlation funnel 
# - Will get a warning because data is highly imbalanced
# - This is why the correlations look so low, but they are actually quite predictive
train_clean_tbl %>%
    binarize() %>%
    rename(target__1 = `target__-OTHER`) %>%
    correlate(target = target__1) %>%
    plot_correlation_funnel(interactive = TRUE)

# Top Features contributing to Bad Loans
# - original_borrower_credit_score: less than 715 is related to bad loans
# - property_state: FL and TX - These states have higher relationship with bad loan
# - original_cltv: 90_Inf - If the Current Loan-to-Value is 90% or more meaning they did not put down a substantial deposit, this increases chance of bad loan l


# 5.0 DISCONNECT ----
connection_close(con_fannie)

