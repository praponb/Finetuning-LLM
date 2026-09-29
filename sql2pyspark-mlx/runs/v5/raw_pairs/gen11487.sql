with ranked as (select *, row_number() over (partition by order_id order by paid_at desc, id) as rn from payments) select * from ranked where rn <= 1 order by rn desc, status;
