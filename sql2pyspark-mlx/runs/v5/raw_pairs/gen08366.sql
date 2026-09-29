select revenue, store_id, units, round(units, 1) as units_rounded from store_sales where sale_date < '2023-10-01' order by units_rounded desc
