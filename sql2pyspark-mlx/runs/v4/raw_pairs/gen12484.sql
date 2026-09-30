with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        level,
        status_code,
        attr_key,
        value
    from app_logs
    lateral view explode(context) li as attr_key, value
),
flattened_attrs as (
    -- Semi-structured parsing
    select
        id,
        level,
        attr_key,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 8) != 0 as int) as has_fault
    from exploded
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        level,
        attr_key,
        status_code_high,
        has_fault,
        sum(has_fault) over (partition by level) as level_sum_has_fault,
        lag(has_fault, 2) over (partition by level order by status_code_high, id, attr_key) as prev_has_fault,
        sum(has_fault) over (partition by attr_key) as attr_key_sum_has_fault
    from flattened_attrs
)
select
    id,
    attr_key,
    status_code_high,
    has_fault,
    level_sum_has_fault,
    prev_has_fault,
    attr_key_sum_has_fault
from with_windows
order by attr_key desc nulls last
