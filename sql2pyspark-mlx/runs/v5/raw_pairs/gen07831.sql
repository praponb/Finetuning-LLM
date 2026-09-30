with scored as (
    -- Window metrics over several different partitions
    select
        id,
        method,
        paid_at,
        status,
        last_value(method) ignore nulls over (partition by order_id order by paid_at, id rows between unbounded preceding and current row) last_known_method,
        row_number() over (order by paid_at desc, id) seq_no,
        ntile(3) over (partition by order_id order by amount, id) amount_bucket,
        lead(amount, 1) over (partition by order_id order by paid_at, id) next_amount
    from payments
)
select
    id,
    method,
    paid_at,
    last_known_method,
    seq_no,
    case when method rlike '^(cash|paypal)$' then 'OTHER_ZONE' when method rlike '^(card|bank_transfer)$' then 'CORE' else 'GROUP_A' end as method_zone
from scored
