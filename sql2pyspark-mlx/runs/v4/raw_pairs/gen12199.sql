with ranked_payments as (select id, order_id, paid_at, rank() over (partition by order_id order by paid_at desc) pos from payments) select * from ranked_payments where pos = 1 order by order_id desc
