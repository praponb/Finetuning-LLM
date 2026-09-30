with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        round(quantity * 1.15, 2) as adjusted_quantity
    from inventory
    where last_restock > date '2023-10-30'
      and store_id in (1, 6)
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        adjusted_quantity,
        dense_rank() over (partition by store_id order by adjusted_quantity) as adjusted_quantity_dense_rank,
        ntile(5) over (partition by product_id, store_id order by adjusted_quantity desc, id) as adjusted_quantity_bucket,
        sum(adjusted_quantity) over (partition by store_id) as store_id_sum_adjusted_quantity,
        row_number() over (partition by store_id order by quantity desc, id) as event_seq
    from filtered
),
joined as (
    -- Attach stores attributes
    select
        src.*,
        b.sqft,
        b.city,
        b.region
    from enriched src
    join stores b
        on src.store_id = b.id
),
joined_inventory as (
    -- Attach products attributes
    select s.*, p.price
    from joined s
    left join products p
        on s.product_id = p.id
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        cast(event_seq as double) as event_seq_str,
        case when price >= 980 then 'High' when price >= 330 then 'Medium' else 'Low' end as price_level
    from joined_inventory
    where adjusted_quantity_dense_rank between 3 and 4
)
-- Native SQL PIVOT
select *
from (
    select
        city,
        region,
        store_id_sum_adjusted_quantity
    from cleaned
) src
pivot (
    sum(store_id_sum_adjusted_quantity) as amount,
    count(1) as cnt
    for region in ('East' as east, 'North' as north)
);
