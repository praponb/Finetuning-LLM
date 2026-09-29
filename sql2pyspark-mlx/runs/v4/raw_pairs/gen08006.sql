select location, dept_name
from departments
where budget >= 3589000
intersect
select location, dept_name
from departments
where location <> 'Chiang Mai'
order by dept_name desc
