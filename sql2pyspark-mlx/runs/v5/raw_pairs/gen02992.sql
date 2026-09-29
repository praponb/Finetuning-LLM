select id, status, order_date, amount, lead(amount, 1) over (partition by status order by order_date, id) next_amount from orders where channel not in ('web', 'mobile') order by 2, amount;
