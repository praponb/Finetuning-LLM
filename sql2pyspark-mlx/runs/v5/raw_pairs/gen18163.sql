with exploded as (
    select
        id,
        customer_id,
        li_item.product_id,
        li_item.qty,
        li_item.promo
    from carts
    lateral view outer explode(items) t as li_item
)
select
    case when qty >= 5 then 'PREMIUM' else 'STANDARD' end as qty_tier,
    size(collect_set(promo)) as num_promos,
    count(*) as cnt,
    size(collect_set(promo)) as num_promos_2,
    round(avg(qty), 2) as avg_qty,
    max(product_id) filter (where promo = 'SPRING10') as spring10_max_product_id,
    count_if(promo = 'SPRING10') as spring10_events
from exploded
group by qty_tier
order by avg_qty desc;
