select *
from (
    select
        location,
        dept_name,
        budget
    from departments
) src
pivot (
    max(budget)
    for dept_name in ('Legal', 'Sales')
);
