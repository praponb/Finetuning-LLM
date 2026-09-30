WITH with_dims AS (
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.sale_date,
        p.region,
        p.sqft
    FROM store_sales src
    LEFT JOIN stores p
        ON src.store_id = p.id
),
scored AS (
    SELECT
        *,
        STDDEV_SAMP(sqft) OVER (PARTITION BY region) AS region_stddev_samp_sqft,
        LAG(sqft, 2) OVER (ORDER BY sale_date ASC, id ASC) AS prev_sqft,
        RANK() OVER (ORDER BY sqft DESC NULLS FIRST) AS sqft_rank
    FROM with_dims
    WHERE YEAR(sale_date) = 2025
),
with_windows AS (
    SELECT
        id,
        sale_date,
        region,
        prev_sqft,
        sqft_rank,
        ROUND(region_stddev_samp_sqft / SUM(region_stddev_samp_sqft) OVER (PARTITION BY store_id), 4) AS region_stddev_samp_sqft_share,
        LEAD(prev_sqft, 2) OVER (PARTITION BY region, store_id ORDER BY prev_sqft ASC, id ASC) AS next_prev_sqft
    FROM scored
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY region ORDER BY prev_sqft ASC) AS rank_in_group
    FROM with_windows
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    sale_date,
    region,
    prev_sqft,
    region_stddev_samp_sqft_share,
    next_prev_sqft,
    rank_in_group
FROM ranked;
