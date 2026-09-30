with base as (
    select
        id,
        amount,
        paid_at,
        status,
        date_format(paid_at, 'MMM yyyy') as paid_at_label
    from payments
    where method is not null
      and paid_at < '2024-10-01'
),
base_payments as (
    select
        id,
        paid_at,
        case when status = 'SUCCESS' then 1 else 0 end as is_success
    from base
    where paid_at >= date '2023-08-05'
),
cleaned as (
    select *, coalesce(is_success, 0) as is_success_filled
    from base_payments
    where paid_at >= date '2023-08-14'
      and hour(paid_at) between 7 and 15
),
remaining_keys as (
    select id
    from cleaned
    except
    select id
    from payments
    where order_id in (2, 3, 13, 28)
),
kept_rows as (
    select s.*
    from cleaned s
    join remaining_keys k on s.id = k.id
),
enriched as (
    select
        *,
        dense_rank() over (order by is_success desc nulls last) as is_success_dense_rank,
        count(is_success_filled) over (order by cast(paid_at as bigint) range between 604800 preceding and current row) as is_success_filled_count_7d
    from kept_rows
),
totals as (
    select
        case when is_success_filled_count_7d >= 16 then 'LARGE' else 'SMALL' end as is_success_filled_count_7d_tier,
        date_trunc('month', paid_at) as activity_month,
        date_trunc('year', paid_at) as year_start,
        round(avg(is_success_filled_count_7d), 2) as avg_is_success_filled_count_7d,
        (unix_timestamp(max(paid_at)) - unix_timestamp(min(paid_at))) / 60 as duration_min,
        count(*) as num_rows,
        max(paid_at) as last_paid_at,
        avg(is_success_filled_count_7d) as avg_is_success_filled_count_7d_2,
        percent_rank() over (partition by case when is_success_filled_count_7d >= 16 then 'LARGE' else 'SMALL' end order by count(*) desc) as num_rows_percent_rank
    from enriched
    group by 1, 2, 3
)
select
    is_success_filled_count_7d_tier,
    activity_month,
    year_start,
    avg_is_success_filled_count_7d,
    duration_min,
    num_rows,
    avg_is_success_filled_count_7d_2
from totals
order by is_success_filled_count_7d_tier nulls last;
