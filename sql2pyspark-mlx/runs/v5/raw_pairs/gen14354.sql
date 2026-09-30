with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        level,
        status_code,
        attr_key,
        attr_value
    from app_logs
    lateral view outer explode(context) e as attr_key, attr_value
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        service,
        host,
        level,
        status_code,
        attr_key,
        attr_value,
        concat(attr_key, ', ', host) as attr_key_host
    from flattened
    where attr_key is distinct from 'version'
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        service,
        level,
        status_code,
        attr_value,
        percent_rank() over (order by status_code desc) as status_code_percent_rank,
        cume_dist() over (partition by attr_key order by status_code desc) as status_code_cume_dist
    from base
),
enriched_app_logs as (
    -- Ranking and gap features
    select
        *,
        stddev_samp(status_code_percent_rank) over (partition by level) as level_stddev_samp_status_code_percent_rank,
        round(status_code / sum(status_code) over (partition by level), 4) as status_code_share,
        dense_rank() over (order by status_code_cume_dist desc nulls first) as status_code_cume_dist_dense_rank,
        percent_rank() over w_part as status_code_percent_rank_2,
        count(status_code_percent_rank) over (partition by service) as service_count_status_code_percent_rank
    from enriched
    window w_part as (order by status_code desc)
),
filtered as (
    -- Basic cleansing and defaults
    select *, round(status_code_cume_dist, 0) as status_code_cume_dist_rounded
    from enriched_app_logs
    where status_code = 414
)
select
    id,
    service,
    level,
    status_code,
    attr_value,
    status_code_percent_rank,
    status_code_share,
    status_code_cume_dist_dense_rank,
    status_code_percent_rank_2
from filtered
