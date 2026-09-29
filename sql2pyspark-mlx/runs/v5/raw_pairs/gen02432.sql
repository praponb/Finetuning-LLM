select case when salary >= 148000 then 'high' when salary >= 44000 then 'mid' else 'low' end as salary_bucket, min(hire_date) as min_hire_date, max(hire_date) as latest_hire_date
from employees
where hire_date between '2024-07-14' and '2025-08-31'
group by case when salary >= 148000 then 'high' when salary >= 44000 then 'mid' else 'low' end
order by latest_hire_date desc, salary_bucket asc
limit 1
