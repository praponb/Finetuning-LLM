with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        plan,
        start_date,
        monthly_fee,
        status,
        round(monthly_fee, 2) as monthly_fee_rounded
    from subscriptions
    where start_date > date '2023-05-06'
),
latest as (
    -- QUALIFY-style window filter
    select id, start_date
    from (
        select *, row_number() over (partition by status order by monthly_fee_rounded nulls first, id) as dedup_rank
        from base
    ) x
    where dedup_rank = 1
),
scored as (
    -- Running and moving aggregates
    select *, row_number() over (order by start_date desc, id) as event_seq
    from latest
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        start_date,
        lead(event_seq, 2) over (order by start_date, id) as next_event_seq,
        lead(event_seq, 1) over (order by event_seq, id) as next_event_seq_2
    from scored
    where month(start_date) in (3, 4, 11)
),
filtered as (
    -- Keep recent, valid rows
    select *, datediff(current_date(), start_date) as days_since_start_date
    from enriched
    where start_date >= date '2023-02-23'
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        start_date,
        next_event_seq,
        days_since_start_date,
        greatest(next_event_seq_2, days_since_start_date) as greatest_next_event_seq_2_days_since_start_date
    from filtered
    where next_event_seq > 3
),
daily as (
    -- Grouped metrics
    select
        case when days_since_start_date >= 90 then 'HIGH' else 'LOW' end as days_since_start_date_tier,
        case when greatest_next_event_seq_2_days_since_start_date >= 82 then 'PREMIUM' else 'STANDARD' end as greatest_next_event_seq_2_days_since_start_date_tier,
        date_trunc('week', start_date) as week_start,
        count(distinct id) as distinct_ids,
        round(avg(greatest_next_event_seq_2_days_since_start_date), 2) as avg_greatest_next_event_seq_2_days_since_start_date,
        count(distinct id) as distinct_ids_2,
        avg(next_event_seq) as avg_next_event_seq
    from cleaned
    group by case when days_since_start_date >= 90 then 'HIGH' else 'LOW' end, case when greatest_next_event_seq_2_days_since_start_date >= 82 then 'PREMIUM' else 'STANDARD' end, date_trunc('week', start_date)
)
select
    days_since_start_date_tier,
    greatest_next_event_seq_2_days_since_start_date_tier,
    week_start,
    avg_greatest_next_event_seq_2_days_since_start_date,
    distinct_ids_2,
    avg_next_event_seq
from daily
order by greatest_next_event_seq_2_days_since_start_date_tier;
