with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view explode(context) li as attr_key, attr_value
),
enriched as (
    -- Ranking and gap features
    select
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value,
        lag(status_code, 1) over (partition by level, attr_key order by status_code, id, attr_key) as prev_status_code,
        ntile(5) over (partition by level, host order by status_code desc, id, attr_key) as status_code_bucket,
        ntile(3) over (order by status_code desc, id, attr_key) as status_code_bucket_2
    from exploded
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        service,
        host,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value,
        prev_status_code,
        status_code_bucket,
        status_code_bucket_2,
        avg(status_code_bucket_2) over (partition by host order by status_code_bucket, id, attr_key rows between 2 preceding and current row) as status_code_bucket_2_avg_2,
        sum(status_code_bucket_2) over (partition by level order by status_code_bucket_2, id, attr_key rows between unbounded preceding and current row) as running_status_code_bucket_2,
        percent_rank() over (order by status_code_bucket_2 desc nulls first) as status_code_bucket_2_percent_rank,
        row_number() over (partition by host order by status_code, id, attr_key) as seq_no
    from enriched
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        sum(status_code_bucket_2_percent_rank) over w_part as cumulative_status_code_bucket_2_percent_rank,
        avg(status_code_bucket_2_avg_2) over (partition by level, attr_key order by status_code_bucket, id, attr_key rows between 5 preceding and current row) as status_code_bucket_2_avg_2_avg_5,
        round(status_code_bucket_2_avg_2 / sum(status_code_bucket_2_avg_2) over (partition by host), 4) as status_code_bucket_2_avg_2_share
    from metrics
    window w_part as (partition by level order by latency_ms, id, attr_key rows between unbounded preceding and current row)
)
-- Ranking and gap features
select
    *,
    sum(status_code_bucket_2_percent_rank) over (partition by attr_key, service order by latency_ms, id, attr_key rows between unbounded preceding and current row) as running_status_code_bucket_2_percent_rank,
    ntile(10) over (partition by attr_key order by status_code_bucket_2_avg_2_avg_5, id, attr_key) as status_code_bucket_2_avg_2_avg_5_bucket,
    count(status_code) over (partition by attr_key) as attr_key_count_status_code
from with_windows
