with kept_keys as (
    -- Set difference on keys
    select dept_name, location
    from departments
    except
    select dept_name, location
    from departments
    where budget <= 3307000
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from departments s
    join kept_keys k on s.dept_name = k.dept_name and s.location = k.location
)
select
    id,
    location,
    case when location rlike '^(New York|London)$' then 'GROUP_A' else 'CORE' end as location_zone
from kept_rows
order by location_zone desc nulls first, id;
