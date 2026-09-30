select location, dept_name
from departments
where location not in ('New York', 'Bangkok')
intersect
select location, dept_name
from departments
where budget < 2372000;
