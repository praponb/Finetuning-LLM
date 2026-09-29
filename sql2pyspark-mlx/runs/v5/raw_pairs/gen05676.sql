with scored as (
    select
        *,
        avg(amount) over w_hist as running_avg_amount,
        min(amount) over w_hist as running_min_amount,
        sum(amount) over w_hist as running_sum_amount,
        count(amount) over w_hist as running_count_amount
    from transactions
    window w_hist as (partition by txn_type order by txn_time, id rows between unbounded preceding and current row)
)
select
    case when running_avg_amount >= 3100 then 'LARGE' else 'SMALL' end as running_avg_amount_tier,
    count(distinct account_id) as distinct_account_ids,
    sum(case when merchant = 'Starbucks' then running_sum_amount else 0 end) as starbucks_running_sum_amount,
    size(collect_set(txn_type)) as num_txn_types
from scored
group by case when running_avg_amount >= 3100 then 'LARGE' else 'SMALL' end
order by distinct_account_ids desc nulls first;
