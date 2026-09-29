with flattened as (
    select
        id,
        host,
        level,
        latency_ms,
        attr_key,
        v
    from app_logs
    lateral view explode(context) t as attr_key, v
)
select
    level,
    sum(case when host = 'h3' then latency_ms else 0 end) as h3_latency_ms,
    approx_percentile(latency_ms, 0.5) as latency_ms_p50,
    bit_and(latency_ms) as latency_ms_bit_and,
    approx_percentile(latency_ms, array(0.25, 0.5, 0.75)) as latency_ms_quartiles,
    min(latency_ms) as min_latency_ms,
    size(collect_set(level)) as num_levels
from flattened
group by level
order by latency_ms_bit_and desc nulls last
