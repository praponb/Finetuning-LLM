select id, method, paid_at, amount, lag(amount, 1, 0) over (partition by method order by paid_at, id) as prev_amount from payments order by prev_amount desc
