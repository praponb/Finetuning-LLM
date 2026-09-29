select id, paid_at, amount, lead(amount, 1) over (order by paid_at, id) as next_amount from payments where method <> 'promptpay' order by paid_at;
