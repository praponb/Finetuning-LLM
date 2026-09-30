select id, units, add_months(sale_date, 3) sale_date_plus_months from store_sales where sale_date <= '2025-11-01' and store_id in (2, 4)
