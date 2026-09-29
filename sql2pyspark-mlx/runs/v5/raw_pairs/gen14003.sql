select id, quantity, last_day(last_restock) as last_restock_month_end from inventory where last_restock >= '2024-01-01' or product_id = 3 order by quantity asc, last_restock_month_end desc
