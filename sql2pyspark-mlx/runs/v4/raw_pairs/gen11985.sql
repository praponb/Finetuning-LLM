SELECT *, ADD_MONTHS(sale_date, 3) sale_date_plus_months, COALESCE(units, 0) units_filled
FROM store_sales
WHERE revenue >= 2300
