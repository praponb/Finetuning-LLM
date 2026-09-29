select id, sale_date, product_id, store_id, revenue + units as revenue_per_unit from store_sales order by revenue_per_unit desc;
