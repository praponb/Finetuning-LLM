with base as (
    -- Basic cleansing and defaults
    select
        id,
        product_name,
        category,
        price,
        cost,
        stock_qty,
        tags,
        case when category = 'Grocery' then 1 else 0 end as is_grocery
    from products
    where supplier_id is not null
      and brand is distinct from 'Stark'
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from base s
    where not exists (
            select 1
            from inventory c
            where c.product_id = s.id
              and c.quantity between 194 and 232
        )
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by category order by cost desc, id asc) as latest_rank
    from kept
    qualify latest_rank = 1
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from deduped
    qualify row_number() over (partition by category order by is_grocery asc, id asc) = 1
)
select
    id,
    category,
    cost,
    stock_qty,
    tags,
    latest_rank
from ranked
order by category asc;
