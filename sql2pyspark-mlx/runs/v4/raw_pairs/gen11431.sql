with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        host,
        level,
        latency_ms,
        status_code,
        k,
        value
    from app_logs
    lateral view explode(context) e as k, value
),
flattened_attrs as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        host,
        k,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 8) != 0 as int) as is_flagged
    from flattened
),
matched_keys as (
    -- Set difference on keys
    select id, host
    from flattened_attrs
    except distinct
    select id, host
    from app_logs
    where latency_ms is not null
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from flattened_attrs s
    inner join matched_keys k on s.id = k.id and s.host = k.host
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        host,
        k,
        status_code_high,
        is_flagged,
        cume_dist() over (partition by host order by status_code_high asc nulls last) as status_code_high_cume_dist,
        cume_dist() over (order by status_code_high desc) as status_code_high_cume_dist_2,
        cume_dist() over (partition by k order by is_flagged asc nulls first) as is_flagged_cume_dist
    from verified
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (partition by k, host order by is_flagged desc nulls first) as is_flagged_percent_rank,
        sum(status_code_high_cume_dist_2) over (partition by host) as host_sum_status_code_high_cume_dist_2
    from scored
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        case when is_flagged >= 1 then 'High' when is_flagged >= 0 then 'Medium' else 'Low' end as is_flagged_tier,
        coalesce(k, 'NONE') as k_filled
    from enriched
    where k is not null
),
scored_app_logs as (
    -- Ranking and gap features
    select
        *,
        count(is_flagged_percent_rank) over (partition by k, host) as k_count_is_flagged_percent_rank,
        rank() over (order by status_code_high_cume_dist_2 asc nulls last) as status_code_high_cume_dist_2_rank,
        cume_dist() over (order by is_flagged_cume_dist asc nulls first) as is_flagged_cume_dist_cume_dist,
        sum(is_flagged_percent_rank) over (partition by host) as host_sum_is_flagged_percent_rank,
        min(is_flagged_cume_dist) over (partition by host) as host_min_is_flagged_cume_dist
    from cleaned
),
stats as (
    -- Grouped metrics
    select
        case when k_count_is_flagged_percent_rank >= 16 then 'PREMIUM' else 'STANDARD' end as k_count_is_flagged_percent_rank_tier,
        count(*) as event_count,
        percentile_approx(host_min_is_flagged_cume_dist, array(0.25, 0.5, 0.75)) as host_min_is_flagged_cume_dist_quartiles,
        sum(status_code_high) filter (where host = 'h2') as h2_total_status_code_high
    from scored_app_logs
    group by 1
    having event_count > 1
)
select
    k_count_is_flagged_percent_rank_tier,
    event_count,
    host_min_is_flagged_cume_dist_quartiles
from stats
order by event_count desc, k_count_is_flagged_percent_rank_tier asc
