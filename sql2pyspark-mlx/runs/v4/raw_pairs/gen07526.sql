with lines as (
    -- Unnest the array into one row per element
    select
        id,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) t as k, attr_value
),
parsed as (
    -- Semi-structured parsing
    select
        level,
        attr_value,
        cast((status_code & 1) != 0 as int) as flag_bit1,
        shiftright(status_code, 2) as status_code_high
    from lines
    where level in ('DEBUG', 'INFO')
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, length(level) as level_length
    from parsed
    where status_code_high != 2
)
select
    attr_value,
    status_code_high,
    level_length
from filtered
order by status_code_high nulls first, level_length;
