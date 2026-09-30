with cleaned as (
    select
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        case when merchant = 'Amazon' then 1 else 0 end as is_amazon,
        coalesce(merchant, 'N/A') as merchant_filled
    from transactions
    where account_id in (15, 35, 39)
),
scored as (
    select
        *,
        first_value(is_amazon, true) over w_grp as first_known_is_amazon,
        avg(is_amazon) over (partition by account_id order by cast(txn_time as bigint) range between 7776000 preceding and current row) as is_amazon_avg_90d,
        row_number() over (order by is_amazon desc, id) as event_seq
    from cleaned
    window w_grp as (order by txn_time, id rows between unbounded preceding and current row)
),
enriched as (
    select
        *,
        ntile(3) over w_running as is_amazon_avg_90d_bucket,
        row_number() over (order by first_known_is_amazon, id) as event_seq_2
    from scored
    window w_running as (order by is_amazon_avg_90d desc, id)
),
with_dims as (
    select e.*, a.opened_date
    from enriched e
    join accounts a
        on e.account_id = a.id
)
select *
from (
    select
        txn_type,
        merchant,
        is_amazon_avg_90d
    from with_dims
) src
pivot (
    max(is_amazon_avg_90d) as value_sum,
    count(1) as cnt
    for merchant in ('7-Eleven' as v_7_eleven, 'Shell' as shell)
);
