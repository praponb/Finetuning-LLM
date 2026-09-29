with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        round(unit_price, 2) as unit_price_rounded,
        unit_price * quantity as line_total
    from order_items
    where product_id is not null
      and quantity between 4 and 6
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by product_id, order_id order by unit_price_rounded desc, id) as row_num
        from filtered
    ) ranked
    where row_num = 1
),
joined as (
    -- Attach products attributes
    select
        a.*,
        b.cost,
        b.brand,
        b.product_name
    from ranked a
    left join products b
        on a.product_id = b.id
    where b.launch_date > '2023-12-01'
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from joined
    qualify rank() over (partition by brand order by row_num) = 1
)
-- Join a dimension and aggregate
select
    o.status,
    o.channel,
    count(*) as rows_total,
    min(src.line_total) as min_line_total
from latest src
join orders o on src.order_id = o.id
group by grouping sets ((o.status, o.channel), (o.status), ())
