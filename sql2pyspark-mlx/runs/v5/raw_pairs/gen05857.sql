select *, concat(dept_name, ' ', location) as dept_name_location
from departments
where dept_name not in ('Engineering', 'HR', 'Marketing');
