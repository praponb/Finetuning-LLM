with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        product_name,
        brand,
        cost,
        launch_date,
        supplier_id,
        concat(category, '/', product_name) category_product_name,
        coalesce(brand, 'N/A') brand_filled
    from products
    where stock_qty <= 397
      and supplier_id is not null
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from filtered
    qualify row_number() over (partition by supplier_id, brand order by launch_date desc, id asc) = 1
),
latest_products as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by supplier_id, brand order by launch_date asc, id asc) as rank_in_group
    from latest
    qualify rank_in_group = 1
),
enriched as (
    -- Ranking and gap features
    select
        id,
        brand,
        cost,
        supplier_id,
        brand_filled,
        rank_in_group,
        lead(cost) over (partition by brand order by cost asc, id asc) next_cost,
        lag(cost) over (partition by brand order by cost asc, id asc) prev_cost,
        row_number() over (partition by brand order by cost asc, id asc) event_seq,
        round(cost / sum(cost) over (partition by supplier_id), 4) cost_share
    from latest_products
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from enriched
    qualify row_number() over (partition by brand order by rank_in_group desc, id asc) = 1
),
stats as (
    -- Grouped metrics
    select
        supplier_id,
        brand,
        case when cost_share >= 0.65 then 'HIGH' else 'LOW' end as cost_share_tier,
        approx_percentile(event_seq, array(0.25, 0.5, 0.75)) as event_seq_quartiles,
        round(sum(cost_share) / nullif(count(case when brand = 'Umbrella' then 1 end), 0), 2) as cost_share_per_umbrella
    from ranked
    group by grouping sets ((supplier_id, brand, case when cost_share >= 0.65 then 'HIGH' else 'LOW' end), (supplier_id, brand), ())
)
select
    brand,
    cost_share_tier,
    cost_share_per_umbrella,
    case when brand rlike '^(Umbrella|Initech)$' then 'OTHER_ZONE' else 'GROUP_A' end as brand_zone
from stats
order by cost_share_tier desc
