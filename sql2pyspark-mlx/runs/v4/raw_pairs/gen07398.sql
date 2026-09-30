select email, case when salary >= 160000 then 'High' when salary >= 42000 then 'Medium' else 'Low' end as salary_band
from employees
where dept_id in (29, 35) and lower(trim(email)) = 'henry@startup.io'
order by salary_band;
