select * from (select case when amount >= 1370 then 'high' when amount >= 580 then 'mid' else 'low' end as amount_bucket, method, amount from payments)
pivot (count(amount) for method in ('promptpay', 'cash', 'bank_transfer', 'card'))
