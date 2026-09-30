select *, last_day(last_restock) as last_restock_month_end, add_months(last_restock, 3) as last_restock_plus_months from inventory;
