select id, account_id, txn_time, amount, amount - lag(amount, 1, 0) over (partition by account_id order by txn_time, id) amount_change from transactions;
