select
    ss.revenue,
    s.region,
    s.opened_date,
    p.cost,
    round(p.cost * 0.9, 2) as adjusted_cost
from store_sales ss
join stores s on s.id = ss.store_id
join products p on ss.product_id = p.id
where ss.sale_date > '2024-02-01' and p.price >= 1140
order by adjusted_cost;
