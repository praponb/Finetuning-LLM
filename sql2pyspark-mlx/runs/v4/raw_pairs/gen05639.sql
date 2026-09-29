select * from (select city, segment, credit_limit from customers)
pivot (avg(credit_limit) for segment in ('SMB', 'Consumer'))
