select
    e.id,
    e.full_name as employee_full_name,
    e.hire_date as employee_hire_date,
    m.level as manager_level,
    m.is_remote as manager_is_remote
from employees e
left join employees m on e.manager_id = m.id
where m.is_remote and e.level is null
