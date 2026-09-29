select id, paid_at, amount, lag(amount, 1, 0) over (order by paid_at, id) as prev_amount from payments order by paid_at;
