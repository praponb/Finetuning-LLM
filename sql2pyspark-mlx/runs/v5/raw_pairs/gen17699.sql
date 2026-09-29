with numbered as (select *, row_number() over (partition by order_id order by cost, id) as rn from shipments) select id, order_id, cost, status from numbered where rn <= 2
