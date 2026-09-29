select * from (select method, status, amount from payments)
pivot (avg(amount) for status in ('REFUNDED', 'FAILED', 'SUCCESS'))
order by method
