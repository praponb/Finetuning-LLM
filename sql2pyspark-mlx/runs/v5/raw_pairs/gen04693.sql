select *, row_number() over (partition by customer_id, account_type order by balance, id) as dedup_rank from accounts qualify dedup_rank = 1
