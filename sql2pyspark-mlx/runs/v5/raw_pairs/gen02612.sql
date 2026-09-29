select id, sale_date, round(units, 0) as units_rounded, ceil(units) as units_whole from store_sales where product_id in (6, 7, 9) or store_id = 3
