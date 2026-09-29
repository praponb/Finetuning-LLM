select *, row_number() over (partition by level order by salary desc, id) as latest_rank from employees qualify latest_rank = 1
