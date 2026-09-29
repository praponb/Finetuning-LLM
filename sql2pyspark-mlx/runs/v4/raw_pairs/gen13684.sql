with filtered as (
    select
        id,
        product_name,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        array_contains(tags, 'premium') as is_premium,
        coalesce(brand, 'NONE') as brand_filled
    from products
    where cost >= 630
),
eligible as (
    select s.*
    from filtered s
    where exists (
            select 1
            from store_sales x
            where x.product_id = s.id
              and x.sale_date <= s.launch_date
        )
),
filtered_products as (
    select *, round(cost * 100, 2) as cost_adjusted
    from eligible
    where launch_date > to_date('2023-10-30')
      and supplier_id is not null
),
with_latest as (
    select s.*, x.units
    from filtered_products s
    left join lateral (
            select c.units
            from store_sales c
            where c.product_id = s.id
            order by c.units desc, c.id desc
            limit 1
        ) x on true
),
filtered_products_2 as (
    select *, upper(category) as category_upper
    from with_latest
    where launch_date > date '2024-01-07'
),
joined as (
    select e.*, b.supplier_name
    from filtered_products_2 e
    left join suppliers b
        on e.supplier_id = b.id
),
wide as (
    select *
    from (
        select
            supplier_id,
            category,
            cost_adjusted
        from joined
    ) src
    pivot (
        sum(cost_adjusted)
        for category in ('Sports', 'Home', 'Books')
    )
)
select
    s.supplier_id,
    s.Sports,
    s.Home,
    (select sum(x.cost) from products x where x.supplier_id = s.supplier_id) as sum_product_cost
from wide s;
