SELECT id, units, revenue, GREATEST(units, revenue) greatest_units_revenue, ADD_MONTHS(sale_date, 3) sale_date_plus_months FROM store_sales WHERE store_id IN (3, 5);
