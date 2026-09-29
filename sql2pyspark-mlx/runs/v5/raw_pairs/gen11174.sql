select * from (select *, row_number() over (partition by level order by salary desc, id) as row_num from employees) ranked where row_num = 1;
