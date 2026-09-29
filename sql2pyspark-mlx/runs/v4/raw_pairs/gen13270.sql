with numbered as (select *, dense_rank() over (partition by customer_id order by amount desc) as rn from orders where customer_id is null) select * from numbered where rn <= 3 order by id asc
