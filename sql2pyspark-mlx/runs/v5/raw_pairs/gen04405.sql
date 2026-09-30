with parsed as (
    select
        id,
        service,
        log_ts,
        payload_json,
        get_json_object(payload_json, '$.user.tier') as user_tier,
        context['region'] as region,
        shiftright(status_code, 2) as status_code_high,
        get_json_object(payload_json, '$.duration') as duration_text,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec
    from app_logs
)
select
    service,
    date_trunc('day', log_ts) as activity_day,
    case when status_code_high >= 2 then 'LARGE' else 'SMALL' end as status_code_high_tier,
    count(case when service = 'billing' then 1 end) as billing_count,
    count_if(service = 'auth') as auth_events
from parsed
group by grouping sets ((service, date_trunc('day', log_ts), case when status_code_high >= 2 then 'LARGE' else 'SMALL' end), (service, date_trunc('day', log_ts)), (service), ())
order by billing_count desc;
