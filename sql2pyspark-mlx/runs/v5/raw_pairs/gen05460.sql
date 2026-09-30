select * from (select method, status, amount from payments) src pivot (avg(amount) as amount for status in ('FAILED' as failed, 'SUCCESS' as success, 'REFUNDED' as refunded))
