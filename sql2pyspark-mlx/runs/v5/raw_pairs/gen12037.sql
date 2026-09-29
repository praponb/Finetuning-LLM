with unpacked as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        service,
        host,
        latency_ms,
        status_code,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier as tier,
        context['region'] as region,
        cast((status_code & 1) != 0 as int) as is_flagged
    from app_logs
    where latency_ms != 1834
)
-- Multi-level aggregation
select
    case when latency_ms >= 2099 then 'LARGE' else 'SMALL' end as latency_ms_tier,
    count(*) filter (where tier = 'pro') as pro_rows,
    approx_percentile(is_flagged, 0.95) as is_flagged_p95,
    bit_and(latency_ms) as latency_ms_bit_and,
    approx_percentile(is_flagged, 0.9) as is_flagged_p90
from unpacked
group by case when latency_ms >= 2099 then 'LARGE' else 'SMALL' end
order by latency_ms_bit_and desc nulls first;
