select id, account_id, txn_time, amount from (select id, account_id, txn_time, amount, rank() over (partition by account_id order by txn_time) as rnk from transactions) as ranked where rnk <= 1
