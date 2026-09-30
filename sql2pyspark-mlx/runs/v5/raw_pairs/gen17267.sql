with valid as (
    -- Basic cleansing and defaults
    select
        *,
        case when dept_name = 'Legal' then 1 else 0 end as is_legal,
        coalesce(budget, 0) as budget_filled,
        coalesce(location, 'OTHER') as location_filled
    from departments
    where location is not null
      and dept_name is distinct from 'Marketing'
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        dept_name,
        location,
        budget,
        budget_filled,
        case when dept_name = 'Legal' then 1 else 0 end as is_legal_2
    from valid
    where dept_name is not null
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        concat(dept_name, ' ', location) as dept_name_location,
        is_legal_2 / nullif(budget_filled, 0) as is_legal_2_per_budget_filled
    from cleaned
    where dept_name is not distinct from 'Engineering'
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        avg(budget_filled) over (partition by dept_name order by budget, id rows between 6 preceding and 1 preceding) as budget_filled_avg_6_prior,
        row_number() over (partition by dept_name order by is_legal_2_per_budget_filled desc, id) as seq_no
    from filtered
)
select
    id,
    dept_name,
    location,
    is_legal_2,
    dept_name_location,
    is_legal_2_per_budget_filled,
    budget_filled_avg_6_prior,
    seq_no,
    case when location rlike '^(Chiang Mai|London)$' then 'PRIORITY' when location rlike '^(Bangkok|New York)$' then 'CORE' else 'GROUP_A' end as location_zone
from with_windows
order by seq_no desc nulls last, is_legal_2_per_budget_filled;
