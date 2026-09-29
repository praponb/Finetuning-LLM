with valid as (
    -- Keep recent, valid rows
    select
        *,
        case when country in ('DE', 'JP') then 'Tier 1' else 'Other' end as country_group,
        coalesce(city, 'N/A') as resolved_city
    from suppliers
    where city is not null
      and rating > 3.3
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        country_group,
        resolved_city,
        case when country in ('DE', 'UK') then 'Priority' else 'Other' end as country_group_2
    from valid
    where city is not null
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        round(rating / sum(rating) over (partition by city), 4) as rating_share,
        lag(rating, 1, 0) over (order by rating, id) as prev_rating,
        row_number() over (partition by country order by rating, id) as row_seq
    from filtered
),
filtered_suppliers as (
    -- Basic cleansing and defaults
    select
        id,
        supplier_name,
        country,
        city,
        country_group,
        country_group_2,
        prev_rating,
        row_seq,
        rating_share + row_seq as rating_share_plus_row_seq,
        substring(supplier_name, 1, 2) as supplier_name_prefix,
        coalesce(city, 'NONE') as city_clean
    from scored
    where city is not null
      and row_seq > 5
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from filtered_suppliers s
    where not exists (
            select 1
            from products ch
            where ch.supplier_id = s.id
              and ch.stock_qty = 111
        )
),
with_last as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select s.*, x.product_launch_date
    from kept s
    join lateral (
            select c.launch_date as product_launch_date
            from products c
            where c.supplier_id = s.id
            order by c.price desc, c.id desc
            limit 1
        ) x on true
),
with_last_suppliers as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.cost,
        latest.product_stock_qty
    from with_last s
    join lateral (
            select c.cost, c.stock_qty as product_stock_qty
            from products c
            where c.supplier_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) latest on true
)
select
    id,
    supplier_name,
    city,
    prev_rating,
    row_seq,
    supplier_name_prefix,
    city_clean,
    cost,
    product_stock_qty
from with_last_suppliers;
