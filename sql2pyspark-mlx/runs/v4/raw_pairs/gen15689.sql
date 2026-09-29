with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        level,
        status_code,
        key,
        value
    from app_logs
    lateral view outer explode(context) e as key, value
),
valid as (
    -- Basic cleansing and defaults
    select *, round(status_code, 0) status_code_rounded
    from exploded
    where status_code < 304
),
metrics as (
    -- Ranking and gap features
    select
        *,
        rank() over (partition by key, level order by status_code) status_code_rank,
        round(status_code / sum(status_code) over (partition by level), 4) status_code_share,
        sum(status_code_rounded) over (partition by level, key) level_sum_status_code_rounded
    from valid
),
valid_app_logs as (
    -- Basic cleansing and defaults
    select *, round(status_code / status_code_rank, 2) status_code_per_status_code_rank
    from metrics
    where status_code_share >= 0.65
),
valid_app_logs_2 as (
    -- Keep recent, valid rows
    select
        id,
        level,
        status_code,
        key,
        value,
        status_code_share,
        level_sum_status_code_rounded,
        status_code_per_status_code_rank,
        round(status_code_per_status_code_rank, 2) status_code_per_status_code_rank_rounded,
        upper(level) level_upper,
        coalesce(key, 'DIRECT') resolved_key
    from valid_app_logs
    where status_code = 409
),
grouped as (
    -- Grouped metrics
    select
        case when status_code_share >= 0.89 then 'PREMIUM' else 'STANDARD' end as status_code_share_tier,
        count(*) as cnt,
        count(distinct key) as distinct_keys,
        sum(case when level = 'ERROR' then status_code else 0 end) as error_status_code,
        sum(case when level = 'ERROR' then level_sum_status_code_rounded else 0 end) as error_level_sum_status_code_rounded,
        sum(status_code_per_status_code_rank_rounded) as total_status_code_per_status_code_rank_rounded,
        approx_percentile(status_code_per_status_code_rank, 0.9) as status_code_per_status_code_rank_p90
    from valid_app_logs_2
    group by 1
)
select
    status_code_share_tier,
    distinct_keys,
    total_status_code_per_status_code_rank_rounded,
    status_code_per_status_code_rank_p90
from grouped
order by total_status_code_per_status_code_rank_rounded desc nulls first
