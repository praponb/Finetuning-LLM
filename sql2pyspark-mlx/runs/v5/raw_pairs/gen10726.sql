with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        latency_ms,
        status_code,
        key,
        value
    from app_logs
    lateral view outer explode(context) t as key, value
),
enriched as (
    -- Ranking and gap features
    select
        id,
        service,
        status_code,
        key,
        count(status_code) over (partition by service, key) as service_count_status_code
    from lines
),
enriched_app_logs as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by key, service order by service_count_status_code nulls last) as service_count_status_code_percent_rank,
        rank() over (partition by key order by service_count_status_code desc nulls last) as service_count_status_code_rank
    from enriched
    where service = 'search'
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        least(service_count_status_code_percent_rank, service_count_status_code) as least_service_count_status_code_percent_rank_service_count_status_code,
        coalesce(key, 'UNKNOWN') as key_clean
    from enriched_app_logs
    where key is not null
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        service,
        key,
        service_count_status_code,
        service_count_status_code_percent_rank,
        service_count_status_code_rank,
        least_service_count_status_code_percent_rank_service_count_status_code,
        key_clean,
        upper(key) as key_upper,
        coalesce(key, 'DIRECT') as key_clean_2
    from cleaned
    where service = 'auth'
),
cleaned_app_logs as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        lower(service) as service_lower,
        coalesce(key, 'N/A') as resolved_key
    from filtered
    where key is not null
      and key is distinct from 'trace'
),
enriched_app_logs_2 as (
    -- Ranking and gap features
    select
        service,
        service_count_status_code,
        service_count_status_code_rank,
        least_service_count_status_code_percent_rank_service_count_status_code,
        key_clean,
        service_lower,
        resolved_key,
        dense_rank() over (partition by service order by service_count_status_code_percent_rank desc nulls first) as service_count_status_code_percent_rank_dense_rank,
        min(service_count_status_code_rank) over (partition by service) as service_min_service_count_status_code_rank,
        cume_dist() over (partition by service order by least_service_count_status_code_percent_rank_service_count_status_code desc nulls last) as least_service_count_status_code_percent_rank_service_count_status_code_cume_dist,
        percent_rank() over (partition by service order by service_count_status_code_rank desc nulls last) as service_count_status_code_rank_percent_rank,
        dense_rank() over (order by service_count_status_code_rank desc) as service_count_status_code_rank_dense_rank
    from cleaned_app_logs
)
select
    service_count_status_code,
    service_count_status_code_rank,
    least_service_count_status_code_percent_rank_service_count_status_code,
    key_clean,
    service_lower,
    resolved_key,
    service_min_service_count_status_code_rank,
    least_service_count_status_code_percent_rank_service_count_status_code_cume_dist,
    service_count_status_code_rank_dense_rank
from enriched_app_logs_2
order by service_min_service_count_status_code_rank nulls first;
