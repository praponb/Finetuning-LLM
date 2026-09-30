select * from (select id, product_id, sale_date, units, row_number() over (partition by product_id order by sale_date, id) as rn from store_sales) as ranked where rn = 1;
