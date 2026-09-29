select id, budget
from departments
where budget > (select avg(budget) from departments)
order by budget desc, id;
