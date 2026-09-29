with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        length(browser) as browser_length
    from sessions
    where started_at >= date '2023-06-03'
),
joined as (
    -- Attach customers attributes
    select
        src.*,
        c.email,
        c.signup_date,
        c.credit_limit
    from filtered src
    left join customers c
        on src.customer_id = c.id
),
with_dims as (
    -- Attach customers attributes
    select src.*, p.name
    from joined src
    left join customers p
        on src.customer_id = p.id
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when started_at - lag(started_at) over (partition by customer_id order by started_at, id) > interval '90' day then 1 else 0 end as is_new_burst
    from with_dims
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        customer_id,
        started_at,
        country,
        signup_date,
        sum(is_new_burst) over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from sessionized
    qualify row_number() over (partition by customer_id, country order by signup_date desc nulls last, id) = 1
),
scored as (
    -- Ranking and gap features
    select
        *,
        lead(burst_id, 1, 0) over (partition by country order by started_at, id) as next_burst_id,
        dense_rank() over (partition by customer_id order by burst_id desc) as burst_id_dense_rank
    from ranked
)
-- Aggregate with conditional metrics
select
    country,
    case when burst_id >= 8 then 'LARGE' else 'SMALL' end as burst_id_tier,
    avg(next_burst_id) as avg_next_burst_id,
    approx_percentile(burst_id_dense_rank, 0.95) as burst_id_dense_rank_p95,
    sum(case when country = 'US' then burst_id else 0 end) as us_burst_id,
    sum(next_burst_id) as total_next_burst_id,
    sort_array(collect_set(country)) as country_list
from scored
group by country, case when burst_id >= 8 then 'LARGE' else 'SMALL' end;
