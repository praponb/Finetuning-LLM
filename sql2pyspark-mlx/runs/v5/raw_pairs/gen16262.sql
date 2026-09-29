with metrics as (
    select
        id,
        method,
        amount,
        paid_at,
        sum(amount) over (order by paid_at, id rows between unbounded preceding and current row) as running_amount,
        sum(amount) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as running_amount_2
    from payments
),
latest as (
    select *, rank() over (partition by method order by amount desc nulls last) as rank_in_group
    from metrics
    qualify rank_in_group = 1
),
base as (
    select
        id,
        method,
        amount,
        paid_at,
        running_amount,
        running_amount_2,
        rank_in_group,
        trim(method) as clean_method
    from latest
    where method is not null
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by method order by running_amount desc nulls first, id) as rank_in_group_2
        from base
    ) t
    where rank_in_group_2 = 1
)
select
    id,
    method,
    amount,
    paid_at,
    running_amount,
    rank_in_group,
    clean_method,
    rank_in_group_2
from ranked;
