with ranked as (select *, rank() over (partition by product_id order by quantity) pos from order_items where product_id is null) select * from ranked where pos = 1 order by pos desc
