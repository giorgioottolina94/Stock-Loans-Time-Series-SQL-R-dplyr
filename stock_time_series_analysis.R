# STOCK ANALYSIS - SQL TIME SERIES

library(tidyverse)
library(lubridate)
library(tidyquant)
library(plotly)

library(dbplyr)

library(DBI)
library(RSQLite)
library(connections) # remotes::install_github("edgararuiz/connections")

con_stocks <- connection_open(drv = SQLite(), dbname = "stocks.sqlite")


# DATA ----
g <- tbl(con_stocks, "stock_history") %>%
    collect() %>%
    
    mutate(date = ymd(date)) %>%
    
    ggplot(aes(date, adjusted, color = symbol)) +
    geom_line() +
    theme_tq() +
    scale_color_tq()

ggplotly(g)


# 1.0 LAGS & DIFFS ---- 

# Prepare SQL Query w/ dbplyr
lags_diffs_query <- tbl(con_stocks, "stock_history") %>%
    
    select(symbol, date, adjusted) %>%
    
    group_by(symbol) %>%
    
    window_order(date) %>%
    
    mutate(lag_1      = lag(adjusted, n = 1)) %>%
    mutate(diff_1     = adjusted - lag_1) %>%
    mutate(pct_diff_1 = diff_1 / lag_1) %>%
    mutate(growth     = cumsum(pct_diff_1) + 1) %>%
    
    ungroup() %>%
    filter(!is.na(lag_1)) 

lags_diffs_query %>% show_query()

lags_diffs_query

# Plot Mean Daily Returns
g <- lags_diffs_query %>%
    collect() %>%
    
    ggplot(aes(symbol, pct_diff_1, fill = symbol)) +
    geom_jitter(aes(text = str_glue("Date: {date}
                                    Pct Change: {scales::percent(pct_diff_1)}")), 
                fill = "black", alpha = 0.15) +
    geom_violin(alpha = 0.5) +
    theme_tq() +
    scale_fill_tq()

ggplotly(g, tooltip = "text")

# Plot Wealth
g <- lags_diffs_query %>%
    collect() %>%
    
    mutate(date = ymd(date)) %>%
    
    ggplot(aes(date, growth, color = symbol, group = symbol)) +
    geom_line() +
    theme_tq() +
    scale_color_tq()

ggplotly(g)


# 2.0 WINDOW (ROLLING) FUNCTIONS ----

rolling_window_query <- tbl(con_stocks, "stock_history") %>%
    
    select(symbol, date, adjusted) %>%
    
    group_by(symbol) %>%
    
    window_frame(from = -90, to = 0) %>%
    window_order(date) %>%
    
    mutate(roll_avg = mean(adjusted, na.rm = TRUE))  %>%
    
    ungroup()

rolling_window_query %>% show_query()

rolling_window_query

g <- rolling_window_query %>%
    collect() %>%
    
    mutate(date = ymd(date)) %>%
    
    ggplot(aes(x = date, y = adjusted, color = symbol, group = symbol)) +
    geom_line() +
    geom_line(aes(y = roll_avg), color = "blue") +
    scale_color_tq() +
    theme_tq()

ggplotly(g)


# 3.0 TIME-BASED AGGREGATIONS -----

# 3.1 By Month ----
lags_diffs_query %>%
    
    mutate(year_mon = substr(date, start = 1, stop = 7)) %>%
    
    group_by(symbol, year_mon) %>%
    summarize(pct_change = sum(pct_diff_1, na.rm = TRUE)) %>%
    ungroup()


# 3.2 By Year ----
lags_diffs_query %>%
    
    mutate(year = substr(date, start = 1, stop = 4)) %>%
    
    group_by(symbol, year) %>%
    summarize(pct_change = sum(pct_diff_1, na.rm = TRUE)) %>%
    ungroup() 


# 3.3 By Quarter ----
pct_change_qtr_query <- lags_diffs_query %>%
    
    mutate(year = substr(date, start = 1, stop = 4)) %>%
    mutate(mon  = substr(date, start = 6, stop = 7)) %>%
    
    mutate(qtr  = NA) %>%
    mutate(qtr  = ifelse(mon %in% c("01", "02", "03"), "Q1", qtr)) %>%
    mutate(qtr  = ifelse(mon %in% c("04", "05", "06"), "Q2", qtr)) %>%
    mutate(qtr  = ifelse(mon %in% c("07", "08", "09"), "Q3", qtr)) %>%
    mutate(qtr  = ifelse(mon %in% c("10", "11", "12"), "Q4", qtr)) %>%
    
    mutate(year_qtr = sql("year || '-' || qtr")) %>%
    
    group_by(symbol, year_qtr) %>%
    summarize(pct_change = sum(pct_diff_1, na.rm = TRUE)) %>%
    ungroup() 
    

pct_change_qtr_query %>% show_query()

# Plot percentage change
g <- pct_change_qtr_query %>%
    collect() %>%
    
    mutate(date = as.yearqtr(year_qtr, format = "%Y-Q%q") %>% as.Date()) %>%
    
    ggplot(aes(date, pct_change, color = symbol, group = symbol)) +
    geom_line() +
    facet_wrap(~ symbol, ncol = 1) +
    scale_color_tq() +
    theme_tq()

ggplotly(g)


# Plot growth
g <- pct_change_qtr_query %>% 
    collect() %>%
    
    mutate(date = zoo::as.yearqtr(year_qtr, format = "%Y-Q%q") %>% lubridate::as.Date()) %>%
    
    group_by(symbol) %>%
    arrange(year_qtr) %>%
    mutate(growth = cumsum(pct_change) + 1) %>%
    ungroup() %>%
    
    ggplot(aes(date, growth, color = symbol)) +
    geom_line() +
    theme_tq() +
    scale_color_tq()

ggplotly(g)

# DISCONNECT ----
connections::connection_close(con_stocks)
