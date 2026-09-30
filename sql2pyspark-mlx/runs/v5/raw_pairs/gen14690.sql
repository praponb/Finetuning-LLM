select id, product_id, revenue, units, datediff(current_date(), sale_date) as days_since_sale_date from store_sales where units <= 25
