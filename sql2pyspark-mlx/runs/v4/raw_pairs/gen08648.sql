select
    txn_cohort,
    is_flagged,
    stddev_amount,
    avg_amount
from (
    select
        case when year(txn_time) < 2024 then 'before_2024' when year(txn_time) = 2024 then '2024' else 'after_2024' end as txn_cohort,
        is_flagged,
        round(stddev(amount), 2) as stddev_amount,
        round(avg(amount), 2) as avg_amount
    from transactions
    where merchant is not null
    group by case when year(txn_time) < 2024 then 'before_2024' when year(txn_time) = 2024 then '2024' else 'after_2024' end, is_flagged
) as agg
where avg_amount < 36
order by txn_cohort asc
