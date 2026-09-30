with filtered as (
    -- Keep recent, valid rows
    select *, date_trunc('MONTH', start_date) as start_date_period
    from subscriptions
    where plan = 'premium'
),
scored as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        start_date_period,
        first_value(monthly_fee, true) over (partition by status order by monthly_fee, id rows between unbounded preceding and current row) as first_known_monthly_fee,
        avg(monthly_fee) over (partition by customer_id order by cast(start_date_period as bigint) range between 7776000 preceding and current row) as monthly_fee_avg_90d,
        avg(monthly_fee) over (partition by status order by cast(start_date_period as bigint) range between 2592000 preceding and current row) as monthly_fee_avg_30d,
        ntile(10) over (partition by status, plan order by monthly_fee desc, id) as monthly_fee_bucket,
        lag(monthly_fee, 2) over (partition by status order by start_date, id) as prev_monthly_fee
    from filtered
),
enriched as (
    -- Ranking and gap features
    select
        *,
        last_value(monthly_fee_avg_30d, true) over (partition by customer_id order by end_date, id rows between unbounded preceding and current row) as last_known_monthly_fee_avg_30d,
        avg(first_known_monthly_fee) over w_recent as first_known_monthly_fee_avg_2,
        max(monthly_fee_avg_30d) over (partition by customer_id) as customer_id_max_monthly_fee_avg_30d
    from scored
    window w_recent as (partition by status, customer_id order by start_date, id rows between 2 preceding and current row)
),
kept_keys as (
    -- Set intersection on keys
    select status, customer_id
    from enriched
    intersect all
    select status, customer_id
    from subscriptions
    where start_date between '2023-12-01' and '2024-04-01'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join kept_keys k on s.status = k.status and s.customer_id = k.customer_id
),
enriched_subscriptions as (
    -- Attach customers attributes
    select
        e.*,
        p.age,
        p.city
    from kept_rows e
    inner join customers p
        on e.customer_id = p.id
),
joined as (
    -- Attach customers attributes
    select
        a.*,
        d.age as customer_age,
        d.country
    from enriched_subscriptions a
    left join customers d
        on a.customer_id = d.id
),
with_dims as (
    -- Attach customers attributes
    select src.*, p.email
    from joined src
    left join customers p
        on src.customer_id = p.id
)
-- Grouped metrics
select
    date_trunc('month', start_date) as activity_month,
    count(*) as event_count,
    count(distinct customer_id) as distinct_customer_ids,
    count_if(plan = 'standard') as standard_events
from with_dims
group by date_trunc('month', start_date)
