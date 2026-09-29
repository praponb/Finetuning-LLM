with matched_keys as (
    select txn_type, merchant
    from transactions
    except distinct
    select txn_type, merchant
    from transactions
    where merchant in ('Grab', 'Lazada')
),
verified as (
    select s.*
    from transactions s
    join matched_keys k on s.txn_type = k.txn_type and s.merchant = k.merchant
)
select
    date_trunc('quarter', txn_time) as quarter_start,
    approx_percentile(amount, array(0.25, 0.5, 0.75)) as amount_quartiles,
    count(case when merchant = 'Shell' then 1 end) as shell_count,
    sum(case when merchant = 'Starbucks' then amount else 0 end) as starbucks_amount,
    (unix_timestamp(max(txn_time)) - unix_timestamp(min(txn_time))) / 60 as duration_min,
    round(avg(amount), 2) as avg_amount,
    min(txn_time) as first_txn_time
from verified
group by quarter_start
order by avg_amount desc
