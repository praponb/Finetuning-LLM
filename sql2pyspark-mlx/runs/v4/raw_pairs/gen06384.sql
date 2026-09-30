with unpacked as (
    -- Higher-order array functions
    select
        id,
        service,
        host,
        latency_ms,
        status_code,
        coalesce(context['version'], 'unknown') as version_or_default,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').retries as retry_count,
        cast((status_code & 1) != 0 as int) as is_flagged
    from app_logs
)
-- Aggregate with conditional metrics
select
    service,
    host,
    case when is_flagged >= 1 then 'PREMIUM' else 'STANDARD' end as is_flagged_tier,
    size(collect_set(host)) as num_hosts,
    count(case when service = 'search' then 1 end) as search_count,
    avg(retry_count) filter (where service = 'checkout') as checkout_avg_retry_count,
    count(*) filter (where host = 'h2') as h2_rows,
    sum(case when host = 'h4' then retry_count else 0 end) as h4_retry_count
from unpacked
group by service, host, is_flagged_tier
having search_count < 1
order by service desc;
