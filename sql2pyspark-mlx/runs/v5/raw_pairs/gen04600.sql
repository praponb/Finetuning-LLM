with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        latency_ms,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) li as key, attr_value
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        service,
        latency_ms,
        key,
        initcap(trim(key)) as key_title
    from exploded
    where latency_ms between 895 and 1462
),
base as (
    -- Basic cleansing and defaults
    select *, concat(key, '/', service) as key_service
    from valid
    where key = 'region'
),
remaining_keys as (
    -- Set intersection on keys
    select id
    from base
    intersect
    select id
    from app_logs
    where service in ('checkout', 'search')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    join remaining_keys k on s.id = k.id
)
-- Grouped metrics
select
    case when latency_ms >= 1870 then 'LARGE' else 'SMALL' end as latency_ms_tier,
    stddev_samp(latency_ms) as latency_ms_stddev,
    stddev_samp(latency_ms) as latency_ms_stddev_2,
    count(*) filter (where key = 'version') as version_rows,
    count(*) filter (where service = 'search') as search_rows
from reconciled
group by 1;
