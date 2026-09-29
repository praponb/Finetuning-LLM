select * from (select dept_id, level, salary from employees) src pivot (sum(salary) for level in ('L1' as l1, 'L5' as l5))
