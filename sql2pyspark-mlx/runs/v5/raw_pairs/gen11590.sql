with exploded as (
    select
        id,
        service,
        host,
        status_code,
        key,
        v
    from app_logs
    lateral view explode(context) li as key, v
),
parsed as (
    select
        id,
        status_code,
        key,
        v,
        cast((status_code & 1) != 0 as int) as is_flagged,
        shiftright(status_code, 2) as status_code_high
    from exploded
),
ranked as (
    select *, row_number() over (partition by key order by status_code_high, id, key) as rn
    from parsed
    qualify rn = 1
),
base as (
    select *, round(status_code_high, 2) status_code_high_rounded
    from ranked
    where key is not null
),
ranked_app_logs as (
    select *, row_number() over (partition by key order by status_code desc, id, key) as row_num
    from base
    qualify row_num = 1
),
enriched as (
    select
        id,
        status_code,
        key,
        status_code_high,
        status_code_high_rounded,
        count(is_flagged) over (partition by key order by is_flagged, id, key rows between 2 preceding and current row) last3_count_is_flagged,
        min(is_flagged) over (partition by key order by is_flagged, id, key rows between 2 preceding and current row) last3_min_is_flagged,
        sum(is_flagged) over (partition by key order by is_flagged, id, key rows between 2 preceding and current row) last3_sum_is_flagged,
        max(is_flagged) over (partition by key order by is_flagged, id, key rows between 2 preceding and current row) last3_max_is_flagged,
        ntile(4) over (order by status_code_high desc, id, key) status_code_high_bucket
    from ranked_app_logs
),
stats as (
    select
        key,
        count(case when key = 'region' then 1 end) as region_count,
        count(case when key = 'trace' then 1 end) as trace_count,
        sort_array(collect_set(key)) as key_list,
        count(*) as num_rows,
        count(case when key = 'trace' then 1 end) as trace_count_2
    from enriched
    group by key
)
select
    key,
    region_count,
    key_list,
    trace_count_2,
    case when key rlike '^(trace|version)$' then 'CORE' else 'PRIORITY' end as key_zone
from stats
order by region_count desc
