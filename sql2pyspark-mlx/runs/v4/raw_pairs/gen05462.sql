with joined as (
    -- Attach stores attributes
    select src.*, b.city
    from store_sales src
    inner join stores b
        on src.store_id = b.id
),
matched_keys as (
    -- Set difference on keys
    select id
    from joined
    except
    select id
    from store_sales
    where sale_date between '2024-04-19' and '2024-09-19'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from joined s
    inner join matched_keys k on s.id = k.id
),
with_dims as (
    -- Attach stores attributes
    select
        e.*,
        s.opened_date,
        s.store_name
    from verified e
    left join stores s
        on e.store_id = s.id
),
joined_store_sales as (
    -- Attach stores attributes
    select
        s.*,
        b.sqft,
        b.city as store_city
    from with_dims s
    inner join stores b
        on s.store_id = b.id
),
matched_keys_2 as (
    -- Set difference on keys
    select store_id, product_id
    from joined_store_sales
    except all
    select store_id, product_id
    from store_sales
    where store_id = 5
),
verified_store_sales as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from joined_store_sales s
    inner join matched_keys_2 k on s.store_id = k.store_id and s.product_id = k.product_id
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        revenue,
        city,
        opened_date,
        store_name,
        sqft,
        upper(store_name) as store_name_upper,
        date_sub(sale_date, 47) as sale_date_minus_47d,
        coalesce(store_city, 'OTHER') as store_city_filled
    from verified_store_sales
    where year(opened_date) = 2025
)
select
    id,
    store_id,
    revenue,
    city,
    opened_date,
    store_name,
    sqft,
    store_name_upper,
    sale_date_minus_47d
from cleaned
order by sale_date_minus_47d desc nulls last;
