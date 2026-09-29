with valid as (
    select
        id,
        product_name,
        category,
        brand,
        cost,
        round(cost, 2) as cost_rounded,
        coalesce(brand, 'NONE') as brand_clean
    from products
    where launch_date > date '2023-05-29'
      and supplier_id is not null
)
select
    *,
    round(cost / sum(cost) over (partition by brand), 4) as cost_share,
    avg(cost_rounded) over (partition by brand, category order by cost, id rows between 5 preceding and 1 preceding) as cost_rounded_avg_5_prior,
    rank() over (partition by brand, category order by cost_rounded desc nulls last) as cost_rounded_rank
from valid
