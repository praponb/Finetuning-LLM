with latest as (
    select *, row_number() over (partition by status order by monthly_fee, id) as latest_rank
    from subscriptions
    qualify latest_rank = 1
),
scored as (
    select
        *,
        lag(monthly_fee) over w_grp as prev_monthly_fee,
        lead(monthly_fee) over w_grp as next_monthly_fee,
        row_number() over w_grp as event_seq
    from latest
    window w_grp as (partition by plan, customer_id order by start_date, id)
),
with_dims as (
    select s.*, c.name
    from scored s
    inner join customers c
        on s.customer_id = c.id
),
latest_subscriptions as (
    select *, row_number() over (partition by plan, customer_id order by latest_rank, id) as dedup_rank
    from with_dims
    qualify dedup_rank = 1
),
cleaned as (
    select
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        status,
        latest_rank,
        prev_monthly_fee,
        next_monthly_fee,
        event_seq,
        dedup_rank,
        round(next_monthly_fee * 100, 2) as next_monthly_fee_scaled
    from latest_subscriptions
    where end_date > to_date('2023-06-18')
),
deduped as (
    select *, row_number() over (partition by plan order by latest_rank desc, id) as rank_in_group
    from cleaned
    qualify rank_in_group = 1
),
pivoted as (
    select *
    from (
        select
            customer_id,
            status,
            next_monthly_fee_scaled
        from deduped
    ) src
    pivot (
        avg(next_monthly_fee_scaled)
        for status in ('EXPIRED', 'CANCELLED')
    )
)
select customer_id, CANCELLED
from pivoted
order by CANCELLED desc, customer_id
limit 20;
