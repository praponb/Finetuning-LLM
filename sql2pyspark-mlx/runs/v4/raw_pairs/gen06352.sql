select amount as value
from transactions
where txn_type in ('DEBIT', 'REFUND')
union
select unit_price as value
from order_items
where unit_price between 170 and 370;
