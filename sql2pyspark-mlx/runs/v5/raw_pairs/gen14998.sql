with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        product_name,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        replace(product_name, ' ', '-') as product_name_slug,
        coalesce(brand, 'UNKNOWN') as resolved_brand
    from products
    where launch_date >= date '2023-10-02'
),
latest_match as (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.supplier_id,
        s.resolved_brand,
        top1.quantity,
        top1.unit_price
    from cleaned s
    join lateral (
            select c.quantity, c.unit_price
            from order_items c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
),
joined as (
    -- Attach suppliers attributes
    select
        e.id,
        e.supplier_id,
        e.resolved_brand,
        e.unit_price,
        p.supplier_name,
        p.city,
        p.rating
    from latest_match e
    inner join suppliers p
        on e.supplier_id <=> p.id
),
with_dims as (
    -- Attach suppliers attributes
    select e.*, p.rating as supplier_rating
    from joined e
    left join suppliers p
        on e.supplier_id = p.id
    where p.country = 'US'
)
-- Running and moving aggregates
select
    id,
    supplier_id,
    resolved_brand,
    supplier_name,
    rating,
    supplier_rating,
    round(unit_price / sum(unit_price) over (partition by supplier_id), 4) as unit_price_share,
    lead(supplier_rating, 1) over w_recent as next_supplier_rating,
    sum(supplier_rating) over w_running as cumulative_supplier_rating,
    cume_dist() over (order by unit_price nulls last) as unit_price_cume_dist,
    round(rating / sum(rating) over (partition by supplier_id), 4) as rating_share
from with_dims
window w_recent as (order by unit_price, id), w_running as (partition by supplier_id order by rating, id rows between unbounded preceding and current row)
