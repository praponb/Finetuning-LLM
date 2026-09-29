select * from (select *, row_number() over (partition by manager_id, dept_id order by salary, id) as rn from employees) t where rn = 1
