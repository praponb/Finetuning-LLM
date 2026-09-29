select id, budget, rank() over (order by budget) rnk
from departments
