with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        v
    from app_logs
    lateral view explode(context) li as attr_key, v
),
enriched as (
    -- Ranking and gap features
    select
        *,
        ntile(4) over w_ordered as latency_ms_bucket,
        max(latency_ms) over (partition by service order by cast(log_ts as bigint) range between 900 preceding and current row) as latency_ms_max_15m,
        sum(latency_ms) over (partition by host order by status_code, id, attr_key rows between unbounded preceding and current row) as cumulative_latency_ms,
        first_value(attr_key) ignore nulls over w_part as first_known_attr_key
    from flattened
    window w_ordered as (partition by attr_key, service order by latency_ms, id, attr_key), w_part as (partition by service, attr_key order by status_code, id, attr_key rows between unbounded preceding and current row)
),
matched_keys as (
    -- Set intersection on keys
    select host, id
    from enriched
    intersect
    select host, id
    from app_logs
    where to_date(log_ts) < '2024-06-01'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    join matched_keys k on s.host = k.host and s.id = k.id
)
select
    id,
    service,
    host,
    log_ts,
    latency_ms,
    status_code,
    v,
    latency_ms_max_15m,
    cumulative_latency_ms,
    case when service rlike '^(search|checkout)$' then 'EXTENDED' else 'GROUP_B' end as service_zone
from verified
order by cumulative_latency_ms desc;
