with ranked as (select *, rank() over (partition by txn_type order by txn_time desc) as rnk from transactions) select id, txn_type, txn_time, account_id from ranked where rnk <= 2
