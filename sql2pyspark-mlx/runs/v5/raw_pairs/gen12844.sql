select level, count(distinct salary) as unique_managers
from employees
where manager_id in (21, 26, 37)
group by level
having count(distinct salary) > 4
order by unique_managers desc
