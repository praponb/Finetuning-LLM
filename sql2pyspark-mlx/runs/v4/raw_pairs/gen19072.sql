with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view explode(context) t as k, attr_value
),
flattened_attrs as (
    -- Semi-structured parsing
    select
        id,
        k,
        attr_value,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 1) != 0 as int) as has_fault
    from lines
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by k order by has_fault desc nulls first, id, k) as rn
    from flattened_attrs
    qualify rn = 1
)
-- Aggregate with conditional metrics
select
    k,
    count(distinct id) as distinct_ids,
    round(avg(status_code_high), 2) as avg_status_code_high,
    sum(case when k = 'trace' then has_fault else 0 end) as trace_has_fault
from ranked
group by k
having round(avg(status_code_high), 2) >= 0
order by avg_status_code_high;
