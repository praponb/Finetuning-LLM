WITH filtered AS (
    SELECT
        *,
        CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END AS is_chiang_mai,
        ROUND(sqft * 100, 2) AS sqft_scaled
    FROM stores
    WHERE region IS NOT NULL
      AND opened_date > DATE '2023-06-10'
),
scored AS (
    SELECT
        *,
        MAX(sqft_scaled) OVER (PARTITION BY region, city ORDER BY sqft_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_sqft_scaled,
        AVG(sqft_scaled) OVER (PARTITION BY region, city ORDER BY sqft_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_sqft_scaled,
        SUM(sqft_scaled) OVER (PARTITION BY region, city ORDER BY sqft_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_sqft_scaled,
        STDDEV_SAMP(sqft_scaled) OVER (PARTITION BY region) AS region_stddev_samp_sqft_scaled
    FROM filtered
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC NULLS FIRST, id) AS rn
        FROM scored
    ) x
    WHERE rn = 1
),
with_last AS (
    SELECT
        s.*,
        latest.store_sale_revenue,
        latest.store_sale_sale_date
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue, c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    SELECT
        *,
        SUM(running_avg_sqft_scaled) OVER (PARTITION BY region) AS region_sum_running_avg_sqft_scaled,
        AVG(is_chiang_mai) OVER (ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS is_chiang_mai_avg_3_prior,
        NTILE(4) OVER (ORDER BY running_sum_sqft_scaled, id) AS running_sum_sqft_scaled_bucket,
        AVG(region_stddev_samp_sqft_scaled) OVER (PARTITION BY city ORDER BY store_sale_revenue, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS region_stddev_samp_sqft_scaled_avg_3_prior
    FROM with_last
    WHERE running_avg_sqft_scaled < 60
),
scored_stores AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) AS row_seq,
        NTILE(3) OVER (PARTITION BY region ORDER BY running_avg_sqft_scaled DESC, id) AS running_avg_sqft_scaled_bucket,
        AVG(sqft) OVER (PARTITION BY city, region ORDER BY sqft, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS sqft_avg_2_prior
    FROM enriched
)
SELECT
    id,
    is_chiang_mai,
    sqft_scaled,
    running_avg_sqft_scaled,
    region_stddev_samp_sqft_scaled,
    rn,
    store_sale_sale_date,
    region_sum_running_avg_sqft_scaled,
    is_chiang_mai_avg_3_prior
FROM scored_stores
ORDER BY sqft_scaled DESC NULLS LAST, is_chiang_mai_avg_3_prior;
