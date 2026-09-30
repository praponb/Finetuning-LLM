with flattened_attrs as (
    select
        id,
        host,
        log_ts,
        latency_ms,
        payload_json,
        context,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier as tier,
        coalesce(context['version'], 'none') as version_or_default,
        context['region'] as region,
        context['version'] as version
    from app_logs
)
select
    *,
    sum(latency_ms) over (order by latency_ms, id rows between unbounded preceding and current row) as running_latency_ms,
    sum(latency_ms) over (partition by tier order by log_ts, id rows between unbounded preceding and current row) as cumulative_latency_ms,
    max(latency_ms) over (partition by tier order by cast(log_ts as bigint) range between 7200 preceding and current row) as latency_ms_max_2h,
    sum(latency_ms) over (partition by host order by cast(log_ts as bigint) range between 7200 preceding and current row) as latency_ms_sum_2h,
    avg(latency_ms) over (partition by tier order by latency_ms, id rows between 5 preceding and 1 preceding) as latency_ms_avg_5_prior
from flattened_attrs
