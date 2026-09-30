select id, txn_type, round(amount, 2) as amount_rounded
from transactions
order by amount_rounded desc, id
limit 10;
