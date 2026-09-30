select dept_name
from departments
where dept_name not in ('Sales', 'Finance')
union all
select dept_name
from departments
where budget between 2862000 and 3736000
order by dept_name
