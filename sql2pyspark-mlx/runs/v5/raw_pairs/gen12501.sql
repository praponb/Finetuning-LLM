select id, status, method, amount, sum(amount) over (partition by status, method) as status_sum_amount from payments where order_id is not null;
