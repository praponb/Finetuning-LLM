with valid as (
    select *, ceil(quantity) as quantity_whole
    from order_items
    where product_id is not null
      and unit_price < 150
),
joined as (
    select
        e.*,
        p.brand,
        p.product_name,
        p.cost
    from valid e
    join products p
        on e.product_id = p.id
),
filtered as (
    select
        *,
        round(quantity_whole / cost, 2) as quantity_whole_per_cost,
        coalesce(brand, 'NONE') as brand_clean
    from joined
    where brand = 'Initech'
),
scored as (
    select
        *,
        ntile(3) over (order by unit_price, id) as unit_price_bucket,
        ntile(5) over (partition by product_id, brand order by unit_price, id) as unit_price_bucket_2,
        avg(cost) over (order by unit_price, id rows between 5 preceding and 1 preceding) as cost_avg_5_prior
    from filtered
),
valid_order_items as (
    select
        id,
        order_id,
        quantity,
        unit_price,
        quantity_whole,
        brand,
        product_name,
        quantity_whole_per_cost,
        brand_clean,
        unit_price_bucket,
        round(cost * 100, 2) as cost_adjusted,
        least(cost_avg_5_prior, quantity) as least_cost_avg_5_prior_quantity,
        coalesce(brand, 'NONE') as brand_clean_2
    from scored
    where product_id in (18, 37)
),
with_windows as (
    select
        id,
        order_id,
        quantity,
        unit_price,
        quantity_whole,
        brand,
        product_name,
        quantity_whole_per_cost,
        brand_clean,
        unit_price_bucket,
        cost_adjusted,
        least_cost_avg_5_prior_quantity,
        brand_clean_2,
        ntile(10) over (partition by brand order by quantity_whole desc, id) as quantity_whole_bucket,
        lag(quantity, 1) over (partition by brand, order_id order by cost_adjusted, id) as prev_quantity
    from valid_order_items
)
select
    brand,
    sum(least_cost_avg_5_prior_quantity) filter (where brand = 'Stark') as stark_total_least_cost_avg_5_prior_quantity,
    size(collect_set(brand)) as num_brands,
    percentile_approx(quantity_whole, 0.9) as quantity_whole_p90
from with_windows
group by 1
