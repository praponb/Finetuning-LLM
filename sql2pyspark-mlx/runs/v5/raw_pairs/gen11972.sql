with filtered as (
    -- Basic cleansing and defaults
    select *, round(revenue / units, 2) as revenue_per_unit
    from store_sales
    where sale_date > date '2023-08-24'
      and revenue < 3900
),
enriched as (
    -- Attach products attributes
    select
        a.*,
        d.product_name,
        d.price
    from filtered a
    left join products d
        on a.product_id = d.id
),
scored as (
    -- Ranking and gap features
    select
        *,
        round(units / sum(units) over (partition by product_id, store_id), 4) as units_share,
        count(revenue) over w_part as store_id_count_revenue
    from enriched
    window w_part as (partition by store_id)
),
enriched_store_sales as (
    -- Attach products attributes
    select
        src.id,
        src.store_id,
        src.product_id,
        src.sale_date,
        src.revenue,
        src.revenue_per_unit,
        src.product_name,
        src.units_share,
        src.store_id_count_revenue,
        p.cost,
        p.category,
        p.product_name as product_product_name
    from scored src
    left join products p
        on src.product_id = p.id
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by store_id, category order by revenue desc, id) as dedup_rank
    from enriched_store_sales
    qualify dedup_rank = 1
)
-- Native SQL PIVOT
select *
from (
    select
        store_id,
        category,
        cost
    from ranked
) src
pivot (
    max(cost)
    for category in ('Grocery', 'Books')
)
