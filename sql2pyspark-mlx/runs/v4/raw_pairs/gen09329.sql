select *, upper(account_type) as account_type_upper from accounts where opened_date between '2023-12-09' and '2025-07-01' or balance < 36000 order by customer_id
