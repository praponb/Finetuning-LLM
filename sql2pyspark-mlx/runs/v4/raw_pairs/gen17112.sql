with base as (
    select
        *,
        substring(status, 1, 3) as status_prefix,
        coalesce(method, 'N/A') as resolved_method
    from payments
    where to_date(paid_at) >= '2023-08-01'
),
gaps as (
    select *, case when lag(paid_at) over (partition by status order by paid_at, id) is null
                 or paid_at > lag(paid_at) over (partition by status order by paid_at, id) + interval '90' day then 1 else 0 end as new_group_flag
    from base
),
sessionized as (
    select *, sum(new_group_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
valid as (
    select
        id,
        method,
        amount,
        status_prefix,
        resolved_method,
        new_group_flag,
        burst_id,
        least(burst_id, new_group_flag) as least_burst_id_new_group_flag
    from sessionized
    where method is not null
),
cleaned as (
    select
        id,
        method,
        amount,
        status_prefix,
        resolved_method,
        new_group_flag,
        burst_id,
        cast(new_group_flag as string) as new_group_flag_num
    from valid
    where method is not null
)
select
    id,
    method,
    amount,
    status_prefix,
    resolved_method,
    new_group_flag
from cleaned
order by new_group_flag desc nulls last;
