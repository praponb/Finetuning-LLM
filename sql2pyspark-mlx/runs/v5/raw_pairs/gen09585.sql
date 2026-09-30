with joined as (
    -- Attach customers attributes
    select
        a.*,
        c.credit_limit,
        c.age,
        c.city
    from subscriptions a
    left join customers c
        on a.customer_id = c.id
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        row_number() over w_running as row_seq,
        lag(credit_limit, 1) over (partition by city order by monthly_fee, id) as prev_credit_limit
    from joined
    window w_running as (partition by city, status order by credit_limit, id)
),
with_dims as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.start_date,
        a.credit_limit,
        a.city,
        b.segment
    from metrics a
    left join customers b
        on a.customer_id = b.id
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by city, customer_id order by start_date desc nulls first, id) as dedup_rank
    from with_dims
    qualify dedup_rank = 1
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        start_date,
        city,
        segment,
        dedup_rank,
        concat_ws('-', city, segment) as city_segment_key
    from deduped
    where start_date > date '2023-02-26'
      and city is distinct from 'Chiang Mai'
)
-- Aggregate with conditional metrics
select
    city,
    segment,
    count(*) filter (where city = 'London') as london_rows,
    size(collect_set(segment)) as num_segments,
    count(distinct customer_id) as distinct_customer_ids,
    size(collect_set(segment)) as num_segments_2,
    size(collect_set(city)) as num_cities
from valid
group by 1, 2
having num_segments > 1
order by segment desc nulls first;
