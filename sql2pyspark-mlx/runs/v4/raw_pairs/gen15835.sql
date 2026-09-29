select * from (select location, dept_name, budget from departments) src pivot (max(budget) as value_sum, count(1) as cnt for dept_name in ('Marketing', 'Legal'))
