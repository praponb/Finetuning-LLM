select
    *,
    sum(cost) over (order by stock_qty, id rows between unbounded preceding and current row) as cumulative_cost,
    row_number() over (partition by category order by stock_qty desc, id) as row_seq,
    lag(price, 1, 0) over (partition by supplier_id order by cost, id) as prev_price
from products
where array_contains(tags, 'clearance')
