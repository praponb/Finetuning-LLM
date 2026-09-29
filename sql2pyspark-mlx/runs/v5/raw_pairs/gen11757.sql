with flattened as (
    select
        id,
        service,
        level,
        k,
        v
    from app_logs
    lateral view outer explode(context) li as k, v
),
valid as (
    select
        service,
        level,
        k,
        concat_ws('-', service, level) as service_level_key
    from flattened
    where level in ('INFO', 'DEBUG', 'WARN')
),
base as (
    select *, initcap(trim(level)) as level_title
    from valid
    where k is not null
      and k = 'version'
),
base_app_logs as (
    select
        service,
        k,
        service_level_key,
        level_title,
        lower(service) as service_lower,
        length(k) as k_length
    from base
    where k in ('trace', 'version', 'region')
),
scored as (
    select
        *,
        percent_rank() over (order by k_length nulls first) as k_length_percent_rank,
        cume_dist() over (order by k_length) as k_length_cume_dist,
        avg(k_length) over (partition by k, service) as k_avg_k_length,
        rank() over (order by k_length desc) as k_length_rank
    from base_app_logs
),
daily as (
    select
        service,
        case when k_length >= 60 then 'HIGH' else 'LOW' end as k_length_tier,
        size(collect_set(service)) as num_services,
        count(distinct k) as distinct_ks
    from scored
    group by service, case when k_length >= 60 then 'HIGH' else 'LOW' end
)
select
    service,
    k_length_tier,
    num_services,
    distinct_ks
from daily;
