select * from (select merchant, txn_type, amount from transactions) src pivot (sum(amount) as value_sum, count(1) as item_count for txn_type in ('CREDIT' as credit, 'DEBIT' as debit));
