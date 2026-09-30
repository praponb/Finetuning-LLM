with with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        first_value(quantity) ignore nulls over (partition by store_id order by quantity, id rows between unbounded preceding and current row) first_known_quantity,
        first_value(quantity) ignore nulls over w_part first_known_quantity_2,
        avg(quantity) over w_grp quantity_avg_3_prior
    from inventory
    window w_part as (partition by product_id order by quantity, id rows between unbounded preceding and current row), w_grp as (partition by store_id order by last_restock, id rows between 3 preceding and 1 preceding)
),
enriched as (
    -- Attach stores attributes
    select
        s.id,
        s.store_id,
        s.quantity,
        s.first_known_quantity,
        s.first_known_quantity_2,
        b.opened_date,
        b.region
    from with_windows s
    inner join stores b
        on s.store_id = b.id
),
enriched_inventory as (
    -- Attach stores attributes
    select src.*, b.region as store_region
    from enriched src
    inner join stores b
        on src.store_id = b.id
),
enriched_inventory_2 as (
    -- Attach stores attributes
    select a.*, s.store_name
    from enriched_inventory a
    left join stores s
        on a.store_id = s.id
    where s.sqft = 4924
),
enriched_inventory_3 as (
    -- Window metrics over several different partitions
    select
        *,
        lead(quantity) over (partition by store_region order by quantity, id) next_quantity,
        lag(first_known_quantity_2) over (partition by store_region order by quantity, id) prev_first_known_quantity_2,
        row_number() over (partition by store_region order by quantity, id) row_seq
    from enriched_inventory_2
)
select
    id,
    store_id,
    quantity,
    first_known_quantity,
    first_known_quantity_2,
    opened_date,
    store_name,
    prev_first_known_quantity_2,
    row_seq
from enriched_inventory_3
order by first_known_quantity_2
