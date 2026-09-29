select d.dept_name, e.hire_date
from departments d
left outer join employees e on d.id = e.dept_id
where d.location <> 'Singapore';
