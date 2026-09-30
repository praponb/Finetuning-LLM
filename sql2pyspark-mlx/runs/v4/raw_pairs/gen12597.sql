with latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by product_id order by unit_price desc) as row_num
        from order_items
    ) ranked
    where row_num = 1
),
latest_order_items as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by order_id order by unit_price desc, id) as rank_in_group
        from latest
    ) ranked
    where rank_in_group = 1
),
enriched as (
    -- Attach orders attributes
    select
        e.*,
        d.shipping_fee,
        d.order_date
    from latest_order_items e
    left join orders d
        on e.order_id = d.id
),
latest_order_items_2 as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by order_id order by rank_in_group desc, id) as rank_in_group_2
    from enriched
    qualify rank_in_group_2 = 1
),
latest_order_items_3 as (
    -- QUALIFY-style window filter
    select *
    from latest_order_items_2
    qualify row_number() over (partition by order_id, product_id order by row_num desc, id) = 1
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by product_id order by unit_price desc nulls last, id) as latest_rank
        from latest_order_items_3
    ) ranked
    where latest_rank = 1
)
-- Multi-level aggregation
select
    date_trunc('quarter', order_date) as quarter_start,
    approx_percentile(shipping_fee, 0.9) as shipping_fee_p90,
    corr(quantity, unit_price) as corr_quantity_unit_price,
    sum(unit_price) as total_unit_price,
    sum(quantity) as total_quantity
from deduped
group by quarter_start
having total_unit_price < 0
order by corr_quantity_unit_price desc;
