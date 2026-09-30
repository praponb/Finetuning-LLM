select amount, is_flagged, concat_ws('/', merchant, txn_type) as merchant_txn_type_key
from transactions
where amount <= 2000
order by amount;
