with filtered as (
    -- Keep recent, valid rows
    select *, upper(category) as category_upper
    from products
    where launch_date > to_date('2023-07-20')
      and product_name is not null
),
with_dims as (
    -- Attach suppliers attributes
    select
        a.*,
        d.city,
        d.country
    from filtered a
    inner join suppliers d
        on a.supplier_id = d.id
    where d.rating > 3.9
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_dims s
    where not exists (
            select 1
            from store_sales x
            where x.product_id = s.id
              and x.sale_date < s.launch_date
        )
),
enriched as (
    -- Ranking and gap features
    select
        id,
        product_name,
        category,
        price,
        stock_qty,
        tags,
        supplier_id,
        country,
        corr(price, stock_qty) over (partition by city) as corr_price_stock_qty,
        percent_rank() over (partition by supplier_id order by price desc nulls first) as price_percent_rank
    from not_flagged
),
not_flagged_products as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from order_items x
            where x.product_id = s.id
              and x.unit_price between 280 and 300
        )
),
enriched_products as (
    -- Attach suppliers attributes
    select e.*, s.supplier_name
    from not_flagged_products e
    inner join suppliers s
        on e.supplier_id = s.id
),
joined as (
    -- Attach suppliers attributes
    select src.*, d.city
    from enriched_products src
    inner join suppliers d
        on src.supplier_id = d.id
)
-- Aggregate with conditional metrics
select
    country,
    sum(stock_qty) as total_stock_qty,
    round(avg(price_percent_rank), 2) as avg_price_percent_rank,
    count_if(city = 'Chiang Mai') as chiang_mai_events,
    bit_and(stock_qty) as stock_qty_bit_and,
    count_if(country = 'TH') as th_events
from joined
group by country
order by avg_price_percent_rank nulls last
