WITH base AS (
    SELECT
        *,
        CAST(sqft AS STRING) sqft_num,
        COALESCE(region, 'OTHER') resolved_region
    FROM stores
    WHERE opened_date > DATE '2023-08-02'
      AND region IS NOT NULL
),
eligible AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.shipping_fee > 33
        )
),
enriched AS (
    SELECT
        id,
        store_name,
        resolved_region,
        ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date, id) event_seq,
        AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) sqft_avg_3_prior,
        CUME_DIST() OVER (PARTITION BY city ORDER BY sqft DESC NULLS LAST) sqft_cume_dist
    FROM eligible
),
kept AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.store_id IN (3, 5)
        )
)
SELECT
    CASE WHEN event_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_tier,
    PERCENTILE_APPROX(sqft_avg_3_prior, 0.9) AS sqft_avg_3_prior_p90,
    PERCENTILE_APPROX(sqft_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS sqft_cume_dist_quartiles,
    ROUND(AVG(sqft_avg_3_prior), 2) AS avg_sqft_avg_3_prior
FROM kept
GROUP BY CASE WHEN event_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END;
