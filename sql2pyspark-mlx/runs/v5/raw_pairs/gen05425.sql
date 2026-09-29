select * from (select method, status, amount from payments where hour(paid_at) between 6 and 12) pivot (count(amount) for status in ('REFUNDED', 'FAILED', 'SUCCESS')) order by method;
