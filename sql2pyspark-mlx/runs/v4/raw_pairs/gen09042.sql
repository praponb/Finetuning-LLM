select d.location, e.level, e.job_title
from departments d
left outer join employees e on e.dept_id = d.id;
