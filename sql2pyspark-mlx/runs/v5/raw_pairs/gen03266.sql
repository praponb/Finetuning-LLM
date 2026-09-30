select * from (select account_id, txn_type, amount from transactions) src pivot (sum(amount) as amount, count(1) as n for txn_type in ('FEE', 'DEBIT'));
