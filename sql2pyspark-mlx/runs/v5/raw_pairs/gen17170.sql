select * from (select *, dense_rank() over (partition by level order by hire_date desc) as pos from employees where salary >= 128000) as ranked where pos <= 3 order by email
