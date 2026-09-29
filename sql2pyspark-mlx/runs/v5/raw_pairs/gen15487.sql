with flagged as (
    select *, case when lag(paid_at) over (partition by method order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by method order by paid_at, id) + interval 90 days then 1 else 0 end as is_new_burst
    from payments
),
sessionized as (
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        sum(is_new_burst) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as session_id
    from flagged
)
select
    date_trunc('year', paid_at) as year_start,
    round(sum(amount) / nullif(count(case when method = 'bank_transfer' then 1 end), 0), 2) as amount_per_bank_transfer,
    percentile_approx(session_id, 0.5) as session_id_p50,
    size(collect_set(method)) as num_methods,
    count(distinct id) as distinct_ids
from sessionized
group by date_trunc('year', paid_at)
order by distinct_ids
