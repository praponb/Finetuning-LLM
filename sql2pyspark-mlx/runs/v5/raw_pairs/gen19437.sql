with remaining_keys as (
    -- Set difference on keys
    select product_id, id
    from store_sales
    except distinct
    select product_id, id
    from store_sales
    where units = 31
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from store_sales s
    inner join remaining_keys k on s.product_id = k.product_id and s.id = k.id
),
metrics as (
    -- Running and moving aggregates
    select
        store_id,
        product_id,
        units,
        revenue,
        min(units) over (partition by store_id, product_id) as store_id_min_units,
        corr(units, revenue) over (partition by product_id) as corr_units_revenue,
        round(units / sum(units) over (partition by store_id, product_id), 4) as units_share
    from reconciled
),
with_dims as (
    -- Attach products attributes
    select
        s.store_id,
        s.product_id,
        s.units,
        s.units_share,
        p.stock_qty
    from metrics s
    inner join products p
        on s.product_id = p.id
),
filtered as (
    -- Keep recent, valid rows
    select
        *,
        coalesce(stock_qty, 0) as stock_qty_filled,
        floor(units) as units_whole
    from with_dims
    where units between 35 and 38
),
enriched as (
    -- Attach stores attributes
    select
        e.*,
        s.store_name,
        s.city,
        s.sqft
    from filtered e
    inner join stores s
        on e.store_id = s.id
),
cleaned as (
    -- Basic cleansing and defaults
    select
        store_id,
        units_share,
        stock_qty,
        units_whole,
        store_name,
        sqft,
        case when units_share >= 0.68 then 'High' when units_share >= 0.36 then 'Medium' else 'Low' end as units_share_band
    from enriched
    where units_whole between 63 and 71
),
enriched_store_sales as (
    -- Attach stores attributes
    select
        a.store_id,
        a.units_whole,
        a.sqft,
        d.sqft as store_sqft,
        d.city
    from cleaned a
    left join stores d
        on a.store_id = d.id
    where lower(d.store_name) like '%own%'
),
wide as (
    -- Native SQL PIVOT
    select *
    from (
        select
            store_id,
            city,
            store_sqft
        from enriched_store_sales
    ) src
    pivot (
        sum(store_sqft) as amount,
        count(1) as n
        for city in ('Singapore', 'Berlin')
    )
)
select
    s.store_id,
    s.Berlin_amount,
    s.Berlin_n,
    (select min(o2.discount) from orders o2 where o2.store_id = s.store_id) as min_order_discount
from wide s
order by Berlin_amount desc nulls first
