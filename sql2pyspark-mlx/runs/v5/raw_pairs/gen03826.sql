select dept_id, cast(salary as int) as salary_int from employees where is_remote = false and level != 'L2' and manager_id in (3, 7, 19, 28)
