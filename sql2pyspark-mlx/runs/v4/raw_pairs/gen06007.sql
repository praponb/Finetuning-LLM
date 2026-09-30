with unpacked as (
    select
        id,
        host,
        log_ts,
        status_code,
        payload_json,
        coalesce(context['version'], 'direct') as version_or_default,
        get_json_object(payload_json, '$.user.tier') as user_tier,
        context['region'] as region
    from app_logs
    where host in ('h4', 'h1')
)
select
    case when status_code >= 462 then 'LARGE' else 'SMALL' end as status_code_tier,
    count(distinct user_tier) as distinct_user_tiers,
    sum(case when host = 'h4' then status_code else 0 end) as h4_status_code,
    min(status_code) as min_status_code,
    count(distinct id) as distinct_ids
from unpacked
group by case when status_code >= 462 then 'LARGE' else 'SMALL' end
