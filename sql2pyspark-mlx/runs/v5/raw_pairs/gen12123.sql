with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        level,
        latency_ms,
        key,
        v
    from app_logs
    lateral view outer explode(context) e as key, v
),
enriched as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by level order by latency_ms desc) as latency_ms_percent_rank,
        sum(latency_ms) over (partition by key) as key_sum_latency_ms,
        round(latency_ms / sum(latency_ms) over (partition by level), 4) as latency_ms_share
    from lines
),
metrics as (
    -- Ranking and gap features
    select *, round(latency_ms / sum(latency_ms) over (partition by level), 4) as latency_ms_share_2
    from enriched
),
enriched_app_logs as (
    -- Ranking and gap features
    select
        service,
        level,
        key,
        latency_ms_percent_rank,
        key_sum_latency_ms,
        latency_ms_share,
        latency_ms_share_2,
        cume_dist() over (partition by service, key order by latency_ms_percent_rank desc nulls last) as latency_ms_percent_rank_cume_dist
    from metrics
)
-- Ranking and gap features
select
    service,
    key,
    latency_ms_percent_rank,
    key_sum_latency_ms,
    latency_ms_share_2,
    latency_ms_percent_rank_cume_dist,
    sum(latency_ms_share) over (partition by level, service) as level_sum_latency_ms_share,
    count(latency_ms_percent_rank) over (partition by service) as service_count_latency_ms_percent_rank,
    max(latency_ms_share_2) over (partition by service) as service_max_latency_ms_share_2
from enriched_app_logs
