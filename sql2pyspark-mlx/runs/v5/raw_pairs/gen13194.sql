select dept_id, case when salary >= 120000 then 'High' when salary >= 84000 then 'Medium' else 'Low' end as salary_tier from employees where manager_id in (19, 35)
