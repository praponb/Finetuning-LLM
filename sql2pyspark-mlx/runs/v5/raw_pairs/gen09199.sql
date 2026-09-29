select *
from (
    select *, row_number() over (partition by dept_id order by hire_date, id) as rnk
    from employees
) as ranked
where rnk <= 3;
