with base as (
    select
        *,
        coalesce(amount, 0) as amount_filled,
        upper(merchant) as merchant_upper
    from transactions
    where txn_time >= timestamp '2023-10-01 06:00:00'
      and amount < 3700
)
select
    account_id,
    case when amount_filled >= 66 then 'LARGE' else 'SMALL' end as amount_filled_tier,
    round(avg(amount), 2) as avg_amount,
    approx_percentile(amount, array(0.25, 0.5, 0.75)) as amount_quartiles,
    (unix_timestamp(max(txn_time)) - unix_timestamp(min(txn_time))) / 60 as span_minutes,
    sum(case when merchant = 'Starbucks' then amount else 0 end) as starbucks_amount,
    rank() over (partition by account_id order by sum(case when merchant = 'Starbucks' then amount else 0 end) desc) as starbucks_amount_rank
from base
group by account_id, amount_filled_tier
having starbucks_amount >= 0;
