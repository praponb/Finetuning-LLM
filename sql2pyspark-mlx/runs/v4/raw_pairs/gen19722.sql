select * from (select merchant, txn_type, amount from transactions where to_date(txn_time) < '2023-09-05') pivot (avg(amount) for txn_type in ('DEBIT', 'REFUND', 'FEE')) order by merchant;
