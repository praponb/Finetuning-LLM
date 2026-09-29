select id, amount, merchant
from transactions
where amount >= (select avg(amount) from transactions where not is_flagged)
