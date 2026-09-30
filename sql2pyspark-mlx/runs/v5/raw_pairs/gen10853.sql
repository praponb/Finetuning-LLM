select
    dept_id,
    manager_id,
    count(*) filter (where level = 'L3') as l3_rows,
    approx_percentile(salary, 0.5) as salary_p50,
    count(*) filter (where level = 'L2') as l2_rows,
    sum(case when level = 'L3' then salary else 0 end) as l3_salary
from employees
group by dept_id, manager_id
