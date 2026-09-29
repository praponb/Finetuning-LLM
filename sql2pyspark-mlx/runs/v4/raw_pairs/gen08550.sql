select store_id, add_months(sale_date, 6) sale_date_plus_months from store_sales where product_id in (1, 4, 5, 10);
