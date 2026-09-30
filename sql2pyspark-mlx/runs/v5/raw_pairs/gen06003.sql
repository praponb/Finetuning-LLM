SELECT *, LAST_DAY(opened_date) AS opened_date_month_end, ROUND(sqft, 2) AS sqft_rounded FROM stores ORDER BY sqft DESC;
