select merchant, txn_type, sum(amount) as total_amount
from transactions
where amount <= 2800
group by merchant, txn_type;
