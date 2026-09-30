with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_name,
        brand,
        stock_qty,
        tags,
        supplier_id,
        size(tags) as tags_count,
        trim(category) as clean_category,
        coalesce(brand, 'UNKNOWN') as resolved_brand
    from products
    where launch_date >= date '2023-03-30'
      and brand is distinct from 'Umbrella'
),
enriched as (
    -- Attach suppliers attributes
    select
        a.id,
        a.product_name,
        a.tags,
        a.supplier_id,
        a.tags_count,
        a.resolved_brand,
        p.country,
        p.supplier_name
    from base a
    left join suppliers p
        on a.supplier_id = p.id
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from enriched
    qualify rank() over (partition by country, supplier_id order by tags_count desc) = 1
),
enriched_products as (
    -- Window metrics over several different partitions
    select
        *,
        lag(tags_count) over (partition by country, supplier_id order by tags_count, id) as prev_tags_count,
        lead(tags_count) over (partition by country, supplier_id order by tags_count, id) as next_tags_count,
        row_number() over (partition by country, supplier_id order by tags_count, id) as seq_no,
        lead(tags_count, 2) over (partition by country order by tags_count, id) as next_tags_count_2
    from ranked
)
select
    id,
    tags,
    supplier_id,
    tags_count,
    resolved_brand,
    country,
    supplier_name,
    prev_tags_count,
    next_tags_count,
    case when supplier_name rlike '^(Lion City Imports|Global Supply)$' then 'OTHER_ZONE' when supplier_name rlike '^(Siam Parts Co|Tokyo Traders)$' then 'EXTENDED' else 'CORE' end as supplier_name_zone
from enriched_products
order by supplier_name_zone desc;
