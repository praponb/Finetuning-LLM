SELECT YEAR(opened_date) AS opened_year, SUM(sqft) AS total_sqft, COUNT(*) AS cnt, ROUND(SUM(sqft) * 100.0 / SUM(SUM(sqft)) OVER (), 2) AS pct_of_total FROM stores GROUP BY YEAR(opened_date);
