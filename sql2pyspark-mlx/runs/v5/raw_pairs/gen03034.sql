select * from (select *, rank() over (partition by product_id order by quantity desc) as row_num from order_items) as r where row_num <= 3;
