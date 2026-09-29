select sale_date from store_sales union select order_date from orders where discount <= 0.29 order by sale_date;
