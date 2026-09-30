with parsed as (
    select
        id,
        service,
        host,
        level,
        status_code,
        payload_json,
        context,
        coalesce(context['version'], 'direct') as version_or_default,
        size(context) as num_context,
        context['version'] as version,
        coalesce(context['region'], 'none') as region_or_default,
        from_json(payload_json, 'STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>').user.tier as tier
    from app_logs
)
select
    id,
    service,
    host,
    level,
    status_code,
    payload_json,
    context,
    version_or_default,
    num_context,
    version,
    region_or_default,
    tier,
    row_number() over (order by num_context, id) as seq_no,
    row_number() over (order by num_context, id) as seq_no_2,
    ntile(10) over (partition by tier order by num_context, id) as num_context_bucket
from parsed
