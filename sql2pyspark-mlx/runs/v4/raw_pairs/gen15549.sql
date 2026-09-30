with with_windows as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by product_id order by sale_date desc, id) seq_no,
        avg(revenue) over (partition by product_id order by sale_date, id rows between 2 preceding and 1 preceding) revenue_avg_2_prior
    from store_sales
),
enriched as (
    -- Attach stores attributes
    select
        a.*,
        p.store_name,
        p.region,
        p.opened_date
    from with_windows a
    join stores p
        on a.store_id = p.id
),
scored as (
    -- Ranking and gap features
    select
        *,
        round(revenue / sum(revenue) over (partition by region, product_id), 4) revenue_share,
        first_value(region) ignore nulls over (partition by store_id order by revenue, id rows between unbounded preceding and current row) first_known_region
    from enriched
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        region,
        revenue_share,
        revenue
    from scored
) src
unpivot (
    reading for metric in (revenue_share, revenue as REVENUE)
);
