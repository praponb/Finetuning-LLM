select customer_id, count(distinct balance) as unique_account_types, min(balance) as min_balance from accounts group by customer_id order by min_balance, customer_id limit 50
