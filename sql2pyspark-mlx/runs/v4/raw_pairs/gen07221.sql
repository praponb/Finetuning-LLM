select * from (select merchant, txn_type, amount from transactions) pivot (max(amount) for txn_type in ('CREDIT', 'FEE'));
