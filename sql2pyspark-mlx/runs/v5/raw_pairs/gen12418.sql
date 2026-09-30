with flattened_attrs as (
    select
        id,
        customer_id,
        created_at,
        items,
        ship_to,
        attributes,
        size(items) as num_items,
        aggregate(items, 0.0D, (acc, li) -> acc + li.qty * li.price, acc -> round(acc, 2)) as basket_amount
    from carts
)
select
    case when num_items >= 3 then 'LARGE' else 'SMALL' end as num_items_tier,
    (unix_timestamp(max(created_at)) - unix_timestamp(min(created_at))) / 60 as span_minutes,
    round(avg(basket_amount), 2) as avg_basket_amount,
    percentile_approx(basket_amount, 0.5) as basket_amount_p50,
    (unix_timestamp(max(created_at)) - unix_timestamp(min(created_at))) / 60 as duration_min
from flattened_attrs
group by case when num_items >= 3 then 'LARGE' else 'SMALL' end;
