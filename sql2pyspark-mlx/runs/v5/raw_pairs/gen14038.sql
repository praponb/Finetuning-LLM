select *
from (
    select dept_name, location, count(*) as cnt
    from departments
    group by dept_name, location
) as t
where cnt > 7
