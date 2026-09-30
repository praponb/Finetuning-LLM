with deduped as (
    select
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        status
    from (
        select *, rank() over (partition by status, plan order by monthly_fee desc) as rank_in_group
        from subscriptions
    ) ranked
    where rank_in_group = 1
),
cleaned as (
    select *, date_trunc('YEAR', end_date) as end_date_period
    from deduped
    where end_date > date '2023-09-14'
      and customer_id is not null
),
enriched as (
    select
        id,
        customer_id,
        plan,
        monthly_fee,
        end_date_period,
        row_number() over w_grp as event_seq,
        last_value(monthly_fee, true) over w_running as last_known_monthly_fee,
        avg(monthly_fee) over (partition by status order by end_date_period, id rows between 5 preceding and current row) as monthly_fee_avg_5,
        percent_rank() over (order by monthly_fee desc nulls first) as monthly_fee_percent_rank
    from cleaned
    window w_grp as (partition by customer_id, plan order by end_date, id), w_running as (partition by plan order by monthly_fee, id rows between unbounded preceding and current row)
),
matched_keys as (
    select id, customer_id
    from enriched
    except
    select id, customer_id
    from subscriptions
    where monthly_fee < 220
),
verified as (
    select s.*
    from enriched s
    inner join matched_keys k on s.id = k.id and s.customer_id = k.customer_id
),
valid as (
    select
        id,
        customer_id,
        plan,
        end_date_period,
        event_seq,
        last_known_monthly_fee,
        monthly_fee_avg_5,
        monthly_fee_percent_rank,
        substring(plan, 1, 2) as plan_prefix
    from verified
    where monthly_fee_avg_5 != 202
)
select
    dim.country,
    dim.segment,
    a.plan,
    count(*) as cnt,
    sum(a.monthly_fee_avg_5) as total_monthly_fee_avg_5
from valid a
inner join customers dim on a.customer_id = dim.id
group by grouping sets ((dim.country, dim.segment, a.plan), (dim.country, dim.segment), (dim.country));
