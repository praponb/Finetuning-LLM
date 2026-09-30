with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        level,
        status_code,
        attr_key,
        value
    from app_logs
    lateral view explode(context) e as attr_key, value
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from exploded
    qualify rank() over (partition by host order by status_code) = 1
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        host,
        level,
        status_code,
        attr_key,
        value,
        avg(status_code) over (order by status_code, id, attr_key rows between 2 preceding and 1 preceding) as status_code_avg_2_prior,
        last_value(attr_key, true) over (partition by host order by status_code, id, attr_key rows between unbounded preceding and current row) as last_known_attr_key,
        first_value(attr_key, true) over w_grp as first_known_attr_key,
        dense_rank() over (partition by host order by status_code) as status_code_dense_rank
    from deduped
    window w_grp as (order by status_code, id, attr_key rows between unbounded preceding and current row)
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(status_code_dense_rank, true) over (order by status_code_dense_rank, id, attr_key rows between unbounded preceding and current row) as first_known_status_code_dense_rank,
        min(status_code) over (partition by attr_key) as attr_key_min_status_code
    from enriched
)
-- Grouped metrics
select
    attr_key,
    count(*) filter (where host = 'h1') as h1_rows,
    max(attr_key_min_status_code) as max_attr_key_min_status_code,
    round(sum(status_code) / nullif(count(case when attr_key = 'region' then 1 end), 0), 2) as status_code_per_region,
    round(avg(status_code), 2) as avg_status_code
from metrics
group by attr_key
