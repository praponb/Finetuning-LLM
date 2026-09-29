select id, amount
from transactions
where amount <= (select min(amount) from transactions)
order by id asc;
