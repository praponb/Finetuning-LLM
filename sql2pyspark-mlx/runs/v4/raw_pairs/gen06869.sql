with gaps as (
    select *, case when txn_time - lag(txn_time) over (partition by txn_type order by txn_time asc, id asc) > interval '30' day then 1 else 0 end as is_new_session
    from transactions
),
sessions_transactions as (
    select *, sum(is_new_session) over (partition by txn_type order by txn_time asc, id asc rows between unbounded preceding and current row) as island_id
    from gaps
)
select
    case when amount >= 3300 then 'PREMIUM' else 'STANDARD' end as amount_tier,
    date_trunc('year', txn_time) as year_start,
    percentile_approx(island_id, 0.9) as island_id_p90,
    count(distinct txn_type) as distinct_txn_types,
    round(sum(is_new_session) / nullif(count(case when merchant = 'Starbucks' then 1 end), 0), 2) as is_new_session_per_starbucks
from sessions_transactions
group by grouping sets ((case when amount >= 3300 then 'PREMIUM' else 'STANDARD' end, date_trunc('year', txn_time)), (case when amount >= 3300 then 'PREMIUM' else 'STANDARD' end), ())
order by amount_tier desc nulls first;
