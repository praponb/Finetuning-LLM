with exploded as (
    select
        id,
        service,
        host,
        level,
        status_code,
        k,
        value
    from app_logs
    lateral view explode(context) li as k, value
),
base as (
    select
        id,
        service,
        host,
        level,
        k,
        value,
        concat_ws('/', k, level) as k_level_key,
        coalesce(k, 'NONE') as k_clean
    from exploded
    where k in ('trace', 'version', 'region')
),
kept_keys as (
    select host, id
    from base
    except
    select host, id
    from app_logs
    where status_code != 263
),
verified as (
    select s.*
    from base s
    join kept_keys k on s.host = k.host and s.id = k.id
),
valid as (
    select
        *,
        length(host) as host_length,
        case when host in ('h1', 'h3') then 'Priority' else 'Other' end as host_group,
        coalesce(k, 'DIRECT') as k_clean_2
    from verified
    where service in ('checkout', 'search')
),
enriched as (
    select
        id,
        service,
        host,
        level,
        k,
        value,
        k_level_key,
        k_clean,
        host_length,
        host_group,
        k_clean_2,
        avg(host_length) over (partition by k) as k_avg_host_length,
        count(host_length) over (partition by level) as level_count_host_length
    from valid
),
enriched_app_logs as (
    select
        *,
        max(host_length) over (partition by k) as k_max_host_length,
        rank() over (partition by service order by k_avg_host_length) as k_avg_host_length_rank,
        avg(host_length) over (partition by service) as service_avg_host_length,
        dense_rank() over (partition by k order by host_length desc) as host_length_dense_rank,
        stddev_samp(host_length) over (partition by level) as level_stddev_samp_host_length
    from enriched
)
select
    id,
    service,
    host,
    level,
    k,
    value,
    k_level_key,
    k_clean,
    host_length,
    host_group,
    k_clean_2,
    k_avg_host_length,
    level_count_host_length,
    k_max_host_length,
    k_avg_host_length_rank,
    service_avg_host_length,
    host_length_dense_rank,
    level_stddev_samp_host_length,
    cume_dist() over (partition by host order by host_length_dense_rank desc) as host_length_dense_rank_cume_dist,
    cume_dist() over (order by k_avg_host_length nulls last) as k_avg_host_length_cume_dist
from enriched_app_logs;
