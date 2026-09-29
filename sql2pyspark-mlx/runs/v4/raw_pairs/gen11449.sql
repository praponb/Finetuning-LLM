with latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by product_id, store_id order by revenue desc, id) as row_num
    from store_sales
    qualify row_num = 1
),
enriched as (
    -- Ranking and gap features
    select
        *,
        row_number() over (partition by product_id order by sale_date desc, id) as seq_no,
        cume_dist() over (partition by store_id order by units nulls last) as units_cume_dist,
        rank() over (order by revenue nulls first) as revenue_rank,
        round(revenue / sum(revenue) over (partition by product_id, store_id), 4) as revenue_share
    from latest
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, units_cume_dist - row_num as units_cume_dist_minus_row_num
    from enriched
    where store_id in (3, 4, 5)
)
-- Multi-level aggregation
select
    store_id,
    bit_or(units) as units_bit_or,
    percentile_approx(seq_no, 0.9) as seq_no_p90,
    stddev_samp(revenue) as revenue_stddev
from valid
group by 1
order by store_id desc;
