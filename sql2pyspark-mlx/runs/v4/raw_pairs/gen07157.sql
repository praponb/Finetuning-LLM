select * from employees qualify rank() over (partition by manager_id order by hire_date) = 1
