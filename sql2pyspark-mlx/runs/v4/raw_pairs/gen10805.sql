with top_employees as (select *, row_number() over (partition by manager_id order by hire_date desc, id) as rn from employees) select * from top_employees where rn = 1;
