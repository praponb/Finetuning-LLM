with valid_transactions as (
    select *, case when amount >= 4100 then 'High' when amount >= 800 then 'Medium' else 'Low' end as amount_tier
    from transactions
    where amount between 2100 and 3200
),
amount_bucket_summary as (
    select case when amount >= 4300 then 'high' when amount >= 1800 then 'mid' else 'low' end as amount_bucket, sum(amount) as total_amount
    from valid_transactions
    group by case when amount >= 4300 then 'high' when amount >= 1800 then 'mid' else 'low' end
)
select amount_bucket, total_amount
from amount_bucket_summary
order by total_amount;
