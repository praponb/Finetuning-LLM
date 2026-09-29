with parsed as (
    -- Semi-structured parsing
    select
        id,
        host,
        log_ts,
        status_code,
        payload_json,
        context,
        size(context) as num_context,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec,
        get_json_object(payload_json, '$.duration') as duration_text,
        coalesce(context['version'], 'direct') as version_or_default
    from app_logs
    where host = 'h2'
)
-- Aggregate with conditional metrics
select
    case when duration_sec >= 3.7 then 'LARGE' else 'SMALL' end as duration_sec_tier,
    count(case when host = 'h1' then 1 end) as h1_count,
    min(duration_sec) as min_duration_sec
from parsed
group by case when duration_sec >= 3.7 then 'LARGE' else 'SMALL' end
order by h1_count nulls last;
