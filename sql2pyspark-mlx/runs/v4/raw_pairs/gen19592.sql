select
    d.id,
    d.location,
    count(e.id) as employee_count,
    count(distinct e.salary) as unique_managers
from departments d
left join employees e on d.id = e.dept_id
where e.level is null
group by d.id, d.location
order by unique_managers asc;
