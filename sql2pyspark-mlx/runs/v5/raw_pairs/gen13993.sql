select id, sale_date, datediff(current_date(), sale_date) as days_since_sale_date from store_sales order by id
