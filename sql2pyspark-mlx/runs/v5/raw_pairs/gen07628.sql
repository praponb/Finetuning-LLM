select * from (select id, manager_id, hire_date, row_number() over (partition by manager_id order by hire_date, id) as rn from employees) as r where rn <= 5;
