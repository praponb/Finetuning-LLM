with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        supplier_name,
        country,
        city,
        rating,
        upper(country) as country_upper,
        coalesce(city, 'NONE') as resolved_city
    from suppliers
    where city in ('Bangkok', 'Tokyo')
),
latest_match as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.product_price,
        latest.stock_qty
    from cleaned s
    left join lateral (
            select c.price as product_price, c.stock_qty
            from products c
            where c.supplier_id = s.id
            order by c.launch_date desc, c.id desc
            limit 1
        ) latest on true
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        stock_qty * rating as stock_qty_times_rating,
        coalesce(city, 'NONE') as city_filled
    from latest_match
    where rating between 2.8 and 3.9
),
base as (
    -- Keep recent, valid rows
    select
        id,
        country,
        city,
        country_upper,
        resolved_city,
        product_price,
        stock_qty,
        city_filled,
        substring(supplier_name, 1, 4) as supplier_name_prefix
    from filtered
    where length(supplier_name) >= 12
),
kept_keys as (
    -- Set difference on keys
    select country
    from base
    except
    select country
    from suppliers
    where rating < 3.9
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    join kept_keys k on s.country = k.country
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        country,
        city,
        country_upper,
        resolved_city,
        product_price,
        stock_qty,
        city_filled,
        supplier_name_prefix,
        sum(stock_qty) over (partition by country) as country_sum_stock_qty,
        min(stock_qty) over (partition by city, country) as city_min_stock_qty,
        max(product_price) over (partition by city) as city_max_product_price,
        dense_rank() over (partition by country order by stock_qty nulls first) as stock_qty_dense_rank,
        count(product_price) over (partition by country) as country_count_product_price
    from reconciled
)
select
    id,
    city,
    resolved_city,
    stock_qty,
    supplier_name_prefix,
    country_sum_stock_qty,
    country_count_product_price,
    case when city rlike '^(Tokyo|New York)$' then 'OTHER_ZONE' else 'EXTENDED' end as city_zone
from scored
order by city_zone desc, stock_qty
