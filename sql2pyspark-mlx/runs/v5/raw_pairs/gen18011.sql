select is_remote, sum(case when level = 'L4' then 1 else 0 end) as l4_count from employees where manager_id = 26 group by 1 having count(distinct salary) < 4 order by l4_count;
