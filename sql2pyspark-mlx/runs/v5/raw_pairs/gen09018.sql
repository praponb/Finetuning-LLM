WITH valid AS (
    SELECT
        *,
        ADD_MONTHS(opened_date, 12) opened_date_plus_months,
        COALESCE(region, 'NONE') resolved_region
    FROM stores
    WHERE opened_date > DATE '2023-06-29'
),
enriched AS (
    SELECT
        id,
        sqft,
        opened_date_plus_months,
        resolved_region,
        AVG(sqft) OVER (PARTITION BY city ORDER BY opened_date_plus_months, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) sqft_avg_3_prior,
        SUM(sqft) OVER (ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_sqft,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft NULLS LAST) sqft_dense_rank,
        AVG(sqft) OVER w_recent sqft_avg_5_prior
    FROM valid
    WHERE UPPER(store_name) LIKE '%IRP%'
    WINDOW w_recent AS (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
),
latest_match AS (
    SELECT
        s.*,
        x.inventory_last_restock,
        x.quantity
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date_plus_months
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    SELECT
        *,
        NTILE(3) OVER (ORDER BY cumulative_sqft, id) cumulative_sqft_bucket,
        LAST_VALUE(sqft_avg_3_prior, TRUE) OVER (ORDER BY cumulative_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_sqft_avg_3_prior,
        AVG(sqft) OVER w_hist sqft_avg_5,
        AVG(sqft) OVER w_running sqft_avg_2_prior
    FROM latest_match
    WINDOW w_hist AS (ORDER BY sqft, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), w_running AS (ORDER BY cumulative_sqft, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
eligible AS (
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.product_id = 3
        )
)
SELECT
    DATE_TRUNC('QUARTER', inventory_last_restock) AS quarter_start,
    STDDEV_SAMP(sqft) AS sqft_stddev,
    BIT_AND(sqft) AS sqft_bit_and
FROM eligible
GROUP BY DATE_TRUNC('QUARTER', inventory_last_restock)
ORDER BY sqft_bit_and DESC NULLS FIRST;
