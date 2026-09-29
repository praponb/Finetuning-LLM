select *
from (
    select
        location,
        dept_name,
        budget
    from departments
) src
pivot (
    sum(budget) as amount,
    count(1) as cnt
    for dept_name in ('Sales' as sales, 'Marketing' as marketing)
);
