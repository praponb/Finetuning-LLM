select store_id, min(sale_date) as first_sale_date, count(units) as units_count, min(revenue) as min_revenue from store_sales group by store_id order by first_sale_date
