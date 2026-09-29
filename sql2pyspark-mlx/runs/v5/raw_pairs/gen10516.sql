select sum(case when level = 'L3' then 1 else 0 end) as l3_count, min(hire_date) as min_hire_date from employees where hire_date >= '2023-03-12' and full_name like '%For%';
