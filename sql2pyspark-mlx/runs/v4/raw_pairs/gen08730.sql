select d.budget, d.location, e.manager_id, e.level, e.is_remote from departments d left outer join employees e on d.id = e.dept_id where d.location in ('New York', 'Bangkok') or e.email is not null
