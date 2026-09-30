with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        latency_ms,
        status_code,
        attr_key,
        v
    from app_logs
    lateral view explode(context) t as attr_key, v
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        service,
        host,
        latency_ms,
        status_code,
        attr_key,
        v,
        length(service) as service_length,
        concat_ws('-', service, host) as service_host_key,
        coalesce(attr_key, 'N/A') as resolved_attr_key
    from exploded
    where service != 'auth'
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        status_code + latency_ms as status_code_plus_latency_ms,
        round(latency_ms, 0) as latency_ms_rounded
    from filtered
    where attr_key is not null
      and attr_key in ('version', 'region', 'trace')
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        lead(status_code_plus_latency_ms) over (partition by host order by status_code_plus_latency_ms, id, attr_key) as next_status_code_plus_latency_ms,
        lag(status_code) over (partition by host order by status_code_plus_latency_ms, id, attr_key) as prev_status_code,
        row_number() over (partition by host order by status_code_plus_latency_ms, id, attr_key) as row_seq,
        sum(status_code_plus_latency_ms) over (partition by service order by latency_ms, id, attr_key rows between unbounded preceding and current row) as cumulative_status_code_plus_latency_ms
    from base
),
with_windows_app_logs as (
    -- Running and moving aggregates
    select
        id,
        host,
        latency_ms,
        attr_key,
        service_host_key,
        status_code_plus_latency_ms,
        latency_ms_rounded,
        next_status_code_plus_latency_ms,
        row_seq,
        lag(latency_ms) over w_part as prev_latency_ms,
        lag(cumulative_status_code_plus_latency_ms) over w_part as prev_cumulative_status_code_plus_latency_ms,
        row_number() over w_part as event_seq,
        avg(status_code_plus_latency_ms) over (order by service_length, id, attr_key rows between 3 preceding and 1 preceding) as status_code_plus_latency_ms_avg_3_prior
    from with_windows
    window w_part as (partition by attr_key order by latency_ms, id, attr_key)
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        host,
        attr_key,
        service_host_key,
        status_code_plus_latency_ms,
        latency_ms_rounded,
        row_seq,
        prev_latency_ms,
        prev_cumulative_status_code_plus_latency_ms,
        lag(prev_latency_ms) over w_hist as prev_prev_latency_ms,
        lead(prev_latency_ms) over w_hist as next_prev_latency_ms,
        lead(row_seq) over w_hist as next_row_seq,
        row_number() over w_hist as row_seq_2,
        row_number() over (partition by attr_key order by row_seq desc, id, attr_key) as seq_no
    from with_windows_app_logs
    window w_hist as (partition by host, attr_key order by prev_cumulative_status_code_plus_latency_ms, id, attr_key)
),
scored_app_logs as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by host order by row_seq, id, attr_key) as seq_no_2,
        lag(next_prev_latency_ms, 2) over (partition by host, attr_key order by prev_cumulative_status_code_plus_latency_ms, id, attr_key) as prev_next_prev_latency_ms,
        percent_rank() over (partition by host order by seq_no desc nulls last) as seq_no_percent_rank,
        round(prev_prev_latency_ms / sum(prev_prev_latency_ms) over (partition by attr_key), 4) as prev_prev_latency_ms_share
    from scored
)
-- Grouped metrics
select
    seq_no,
    attr_key,
    host,
    sum(row_seq_2) as total_row_seq_2,
    sum(case when attr_key = 'region' then seq_no_percent_rank else 0 end) as region_seq_no_percent_rank,
    count(*) as cnt,
    count(case when attr_key = 'trace' then 1 end) as trace_count,
    count(distinct host) as distinct_hosts,
    sum(case when host = 'h1' then prev_cumulative_status_code_plus_latency_ms else 0 end) as h1_prev_cumulative_status_code_plus_latency_ms
from scored_app_logs
group by seq_no, attr_key, host
having cnt > 1
