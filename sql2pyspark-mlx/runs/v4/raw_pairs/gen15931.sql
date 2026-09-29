with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        account_id,
        txn_type,
        merchant,
        is_flagged,
        cast(is_flagged as int) as is_flagged_flag,
        ceil(amount) as amount_whole
    from transactions
    where merchant = 'Amazon'
)
-- Multi-level aggregation
select
    case when is_flagged_flag >= 60 then 'LARGE' else 'SMALL' end as is_flagged_flag_tier,
    count(*) as num_rows,
    approx_percentile(amount_whole, 0.9) as amount_whole_p90,
    count(case when txn_type = 'CREDIT' then 1 end) as credit_count
from cleaned
group by is_flagged_flag_tier
order by amount_whole_p90;
