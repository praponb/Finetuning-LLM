with flattened as (
    select
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        value
    from app_logs
    lateral view outer explode(context) t as key, value
)
select
    case when latency_ms >= 2525 then 'PREMIUM' else 'STANDARD' end as latency_ms_tier,
    approx_percentile(latency_ms, 0.5) as latency_ms_p50,
    sum(case when key = 'version' then latency_ms else 0 end) as version_latency_ms,
    count(*) filter (where host = 'h2') as h2_rows
from flattened
group by case when latency_ms >= 2525 then 'PREMIUM' else 'STANDARD' end
having sum(case when key = 'version' then latency_ms else 0 end) > 0;
