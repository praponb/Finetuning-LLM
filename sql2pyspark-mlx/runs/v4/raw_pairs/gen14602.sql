with ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by order_id order by quantity, id) as row_num
        from order_items
    ) x
    where row_num = 1
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by order_id order by quantity, id) as event_seq,
        round(unit_price / sum(unit_price) over (partition by order_id), 4) as unit_price_share
    from ranked
),
kept_keys as (
    -- Set difference on keys
    select product_id, id
    from with_windows
    except distinct
    select product_id, id
    from order_items
    where order_id in (3, 5, 23)
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join kept_keys k on s.product_id = k.product_id and s.id = k.id
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        rank() over (partition by product_id, order_id order by quantity desc nulls last) as quantity_rank,
        max(unit_price_share) over (partition by product_id) as product_id_max_unit_price_share,
        cume_dist() over (partition by product_id order by quantity nulls first) as quantity_cume_dist
    from verified
),
remaining_keys as (
    -- Set difference on keys
    select order_id, product_id
    from enriched
    except distinct
    select order_id, product_id
    from order_items
    where unit_price <= 390
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join remaining_keys k on s.order_id = k.order_id and s.product_id = k.product_id
),
enriched_order_items as (
    -- Attach products attributes
    select a.*, d.launch_date
    from kept_rows a
    inner join products d
        on a.product_id = d.id
),
joined as (
    -- Attach orders attributes
    select
        a.*,
        o.order_date,
        o.status,
        o.shipping_fee
    from enriched_order_items a
    left join orders o
        on a.order_id = o.id
),
by_type as (
    -- Native SQL PIVOT
    select *
    from (
        select
            product_id,
            status,
            unit_price_share
        from joined
    ) src
    pivot (
        sum(unit_price_share) as amount,
        count(1) as item_count
        for status in ('CANCELLED', 'SHIPPED')
    )
)
select
    product_id,
    CANCELLED_amount,
    SHIPPED_amount,
    SHIPPED_item_count
from by_type
