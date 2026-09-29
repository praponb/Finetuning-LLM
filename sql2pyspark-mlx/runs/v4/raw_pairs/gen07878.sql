with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        status_code,
        key,
        attr_value
    from app_logs
    lateral view outer explode(context) x as key, attr_value
),
unpacked as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        status_code,
        key,
        attr_value,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 1) != 0 as int) as is_flagged
    from flattened
    where key in ('trace', 'version')
),
remaining_keys as (
    -- Set difference on keys
    select id
    from unpacked
    except distinct
    select id
    from app_logs
    where service in ('checkout', 'auth')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from unpacked s
    join remaining_keys k on s.id = k.id
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        status_code,
        key,
        attr_value,
        status_code_high,
        is_flagged,
        floor(status_code) as status_code_whole
    from reconciled
    where key is not null
),
scored as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (partition by key order by is_flagged) as is_flagged_percent_rank,
        percent_rank() over w_running as is_flagged_percent_rank_2,
        percent_rank() over (partition by key order by status_code_high desc nulls first) as status_code_high_percent_rank
    from valid
    window w_running as (partition by key order by is_flagged desc nulls last)
)
-- Multi-level aggregation
select
    key,
    case when is_flagged_percent_rank_2 >= 0.77 then 'HIGH' else 'LOW' end as is_flagged_percent_rank_2_tier,
    count_if(key = 'version') as version_events,
    percentile_approx(status_code_high_percent_rank, array(0.25, 0.5, 0.75)) as status_code_high_percent_rank_quartiles,
    sum(case when key = 'region' then is_flagged_percent_rank_2 else 0 end) as region_is_flagged_percent_rank_2,
    grouping(key) as gid
from scored
group by grouping sets ((key, case when is_flagged_percent_rank_2 >= 0.77 then 'HIGH' else 'LOW' end), (key));
