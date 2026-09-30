select distinct merchant, txn_type
from transactions
where merchant in ('Grab', 'Lazada');
