select s.*, latest.salary
from departments s
left join lateral (
        select c.salary
        from employees c
        where c.dept_id = s.id
        order by c.hire_date desc, c.id desc
        limit 1
    ) latest on true;
