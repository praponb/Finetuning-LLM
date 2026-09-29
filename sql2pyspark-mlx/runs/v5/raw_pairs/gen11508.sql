with with_latest as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.supplier_name,
        s.rating,
        latest.cost,
        latest.stock_qty
    from suppliers s
    join lateral (
            select c.cost, c.stock_qty
            from products c
            where c.supplier_id = s.id
            order by c.price desc, c.id desc
            limit 1
        ) latest on true
)
-- Grouped metrics
select
    case when rating >= 3.8 then 'LARGE' else 'SMALL' end as rating_tier,
    count(distinct id) as distinct_ids,
    max(stock_qty) as max_stock_qty,
    percentile_approx(rating, 0.9) as rating_p90,
    percentile_approx(cost, 0.5) as cost_p50,
    max(stock_qty) as max_stock_qty_2
from with_latest
group by case when rating >= 3.8 then 'LARGE' else 'SMALL' end
having max(stock_qty) >= 0;
