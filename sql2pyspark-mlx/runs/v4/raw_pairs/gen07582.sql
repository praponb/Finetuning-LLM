with joined as (
    select src.*, p.brand
    from store_sales src
    left join products p
        on src.product_id = p.id
),
with_windows as (
    select
        *,
        lag(units, 2) over (partition by brand order by sale_date, id) as prev_units,
        ntile(4) over (partition by store_id order by units, id) as units_bucket,
        corr(revenue, units) over (partition by product_id) as corr_revenue_units
    from joined
),
enriched as (
    select
        *,
        dense_rank() over (partition by brand order by units desc) as units_dense_rank,
        sum(revenue) over (order by prev_units, id rows between unbounded preceding and current row) as cumulative_revenue
    from with_windows
)
select
    id,
    sale_date,
    brand,
    prev_units,
    units_bucket,
    corr_revenue_units,
    cumulative_revenue,
    case when brand rlike '^(Stark|Initech)$' then 'GROUP_B' when brand rlike '^(Acme|Globex)$' then 'CORE' else 'GROUP_A' end as brand_zone
from enriched;
