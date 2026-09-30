with ranked as (
    select *
    from products
    qualify row_number() over (partition by category order by stock_qty desc nulls last, id) = 1
),
ranked_products as (
    select *, row_number() over (partition by brand order by stock_qty nulls last, id) as latest_rank
    from ranked
    qualify latest_rank = 1
),
not_flagged as (
    select s.*
    from ranked_products s
    where not exists (
            select 1
            from store_sales ch
            where ch.product_id = s.id
              and ch.sale_date between '2024-04-01' and '2024-07-01'
        )
),
latest_match as (
    select s.*, top1.order_item_quantity
    from not_flagged s
    join lateral (
            select c.quantity as order_item_quantity
            from order_items c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
),
joined as (
    select
        a.*,
        s.rating,
        s.supplier_name,
        s.country
    from latest_match a
    left join suppliers s
        on a.supplier_id = s.id
),
enriched as (
    select
        s.*,
        d.supplier_name as supplier_supplier_name,
        d.country as supplier_country
    from joined s
    join suppliers d
        on s.supplier_id = d.id
),
remaining_keys as (
    select supplier_id
    from enriched
    except distinct
    select supplier_id
    from products
    where size(tags) > 1
),
reconciled as (
    select s.*
    from enriched s
    join remaining_keys k on s.supplier_id = k.supplier_id
)
select
    product_name,
    category,
    brand,
    price,
    cost,
    stock_qty,
    tags,
    supplier_id,
    latest_rank
from reconciled
order by stock_qty;
