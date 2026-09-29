with filtered as (
    -- Keep recent, valid rows
    select
        id,
        country,
        city,
        concat(city, ' - ', supplier_name) as city_supplier_name
    from suppliers
    where city is not null
      and city not in ('London', 'Chiang Mai', 'New York')
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from filtered s
    where not exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and array_contains(x.tags, 'eco')
        )
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        upper(country) as country_upper,
        upper(country) as country_upper_2
    from eligible
    where city is not null
      and city in ('Chiang Mai', 'New York')
),
with_last as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select s.*, top1.price
    from cleaned s
    join lateral (
            select c.price
            from products c
            where c.supplier_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) top1 on true
),
eligible_suppliers as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_last s
    where exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and c.launch_date >= '2025-10-01'
        )
)
-- Grouped metrics
select
    case when price >= 1000 then 'PREMIUM' else 'STANDARD' end as price_tier,
    round(avg(price), 2) as avg_price,
    percentile_approx(price, array(0.25, 0.5, 0.75)) as price_quartiles
from eligible_suppliers
group by price_tier
having round(avg(price), 2) > 0
order by avg_price;
