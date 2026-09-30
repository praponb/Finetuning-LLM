with scored as (
    -- Window metrics over several different partitions
    select
        *,
        lead(quantity, 1, 0) over w_hist next_quantity,
        lag(quantity, 1) over w_hist prev_quantity,
        max(quantity) over w_ordered order_id_max_quantity
    from order_items
    window w_hist as (order by unit_price, id), w_ordered as (partition by order_id, product_id)
),
joined as (
    -- Attach orders attributes
    select
        e.*,
        d.channel,
        d.discount,
        d.shipping_fee
    from scored e
    inner join orders d
        on e.order_id = d.id
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        lead(shipping_fee, 1) over (partition by product_id order by order_id_max_quantity, id) next_shipping_fee,
        percent_rank() over (partition by order_id order by next_quantity nulls last) next_quantity_percent_rank
    from joined
),
enriched as (
    -- Attach products attributes
    select
        src.*,
        b.stock_qty,
        b.launch_date,
        b.price
    from with_windows src
    left join products b
        on src.product_id = b.id
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from enriched
    qualify rank() over (partition by product_id order by shipping_fee desc nulls last) = 1
),
totals as (
    -- Aggregate with conditional metrics
    select
        case when order_id_max_quantity >= 9 then 'LARGE' else 'SMALL' end as order_id_max_quantity_tier,
        count(distinct id) as distinct_ids,
        round(sum(prev_quantity) / nullif(count(case when channel = 'partner' then 1 end), 0), 2) as prev_quantity_per_partner,
        round(avg(discount), 2) as avg_discount,
        round(sum(shipping_fee) / nullif(count(case when channel = 'mobile' then 1 end), 0), 2) as shipping_fee_per_mobile,
        round(avg(discount), 2) as avg_discount_2
    from deduped
    group by case when order_id_max_quantity >= 9 then 'LARGE' else 'SMALL' end
    having avg_discount < 0
)
select
    order_id_max_quantity_tier,
    distinct_ids,
    prev_quantity_per_partner,
    avg_discount,
    shipping_fee_per_mobile,
    avg_discount_2
from totals
order by prev_quantity_per_partner desc nulls first, order_id_max_quantity_tier;
