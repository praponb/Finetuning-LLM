SELECT *, QUARTER(sale_date) sale_date_quarter, ROUND(revenue, 2) revenue_rounded FROM store_sales WHERE revenue <= 4000 AND (revenue IS NOT NULL OR revenue IS NULL)
