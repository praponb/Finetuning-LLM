with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        date_format(last_restock, 'yyyy-MM-dd') as last_restock_label,
        date_format(last_restock, 'yyyy-MM') as last_restock_label_2
    from inventory
    where last_restock > to_date('2023-04-24')
      and quantity <> 97
),
scored as (
    -- Running and moving aggregates
    select
        *,
        lead(quantity) over (partition by product_id, store_id order by quantity, id) as next_quantity,
        row_number() over (partition by product_id, store_id order by quantity, id) as seq_no
    from filtered
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock_label,
        last_restock_label_2,
        seq_no,
        seq_no + quantity as seq_no_plus_quantity
    from scored
    where seq_no <> 5
),
enriched as (
    -- Attach products attributes
    select
        s.id,
        s.product_id,
        d.stock_qty,
        d.product_name,
        d.cost
    from valid s
    join products d
        on s.product_id = d.id
)
select
    id,
    product_id,
    stock_qty,
    product_name,
    cost,
    case when product_name rlike '^(Desk Lamp|Backpack XL)$' then 'GROUP_A' else 'PRIORITY' end as product_name_zone
from enriched
order by id;
