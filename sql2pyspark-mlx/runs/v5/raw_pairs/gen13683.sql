with unpacked as (
    select
        id,
        host,
        context,
        get_json_object(payload_json, '$.user.id') as user_id,
        coalesce(context['region'], 'none') as region_or_default,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').duration as duration_sec,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 4) != 0 as int) as has_fault
    from app_logs
)
select
    case when duration_sec >= 3.7 then 'HIGH' else 'LOW' end as duration_sec_tier,
    count(case when host = 'h1' then 1 end) as h1_count,
    count(*) filter (where host = 'h1') as h1_rows,
    count_if(host = 'h4') as h4_events,
    count(case when host = 'h1' then 1 end) as h1_count_2,
    size(collect_set(host)) as num_hosts
from unpacked
group by duration_sec_tier
order by h4_events desc nulls last
