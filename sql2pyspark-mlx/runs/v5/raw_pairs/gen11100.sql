with valid as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        cast(revenue as int) as revenue_int
    from store_sales
    where revenue >= 1700
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        add_months(sale_date, 6) as sale_date_plus_months
    from valid
    where sale_date >= date '2023-03-23'
),
remaining_keys as (
    -- Set difference on keys
    select product_id, store_id
    from cleaned
    except
    select product_id, store_id
    from store_sales
    where store_id in (1, 2, 6)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    join remaining_keys k on s.product_id = k.product_id and s.store_id = k.store_id
),
kept_keys as (
    -- Set intersection on keys
    select product_id
    from kept_rows
    intersect
    select product_id
    from store_sales
    where sale_date between '2025-01-01' and '2025-02-01'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from kept_rows s
    join kept_keys k on s.product_id = k.product_id
),
kept_keys_2 as (
    -- Set intersection on keys
    select product_id
    from verified
    intersect all
    select product_id
    from store_sales
    where units between 25 and 37
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from verified s
    join kept_keys_2 k on s.product_id = k.product_id
),
matched_keys as (
    -- Set difference on keys
    select store_id, id
    from reconciled
    except distinct
    select store_id, id
    from store_sales
    where store_id = 3
),
reconciled_store_sales as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from reconciled s
    join matched_keys k on s.store_id = k.store_id and s.id = k.id
)
select
    id,
    store_id,
    product_id,
    sale_date
from reconciled_store_sales
order by store_id, id desc
