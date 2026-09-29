select month(opened_date) as opened_month, sum(sqft) as total_sqft, round(sum(sqft) * 100.0 / sum(sum(sqft)) over (), 2) as pct_of_total from stores group by month(opened_date)
