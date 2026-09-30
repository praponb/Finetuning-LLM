WITH cleaned AS (
    SELECT
        *,
        CASE WHEN region = 'South' THEN 1 ELSE 0 END AS is_south,
        COALESCE(sqft, 0) AS sqft_filled
    FROM stores
    WHERE opened_date > TO_DATE('2023-03-31')
      AND region IS NOT NULL
      AND region IS NOT NULL
),
eligible AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date <= s.opened_date
        )
),
base AS (
    SELECT
        id,
        region,
        opened_date,
        sqft,
        is_south,
        sqft_filled,
        GREATEST(is_south, sqft) AS greatest_is_south_sqft
    FROM eligible
    WHERE sqft_filled >= 70
),
base_stores AS (
    SELECT
        id,
        region,
        sqft,
        is_south,
        sqft_filled,
        greatest_is_south_sqft,
        COALESCE(greatest_is_south_sqft, 0) AS greatest_is_south_sqft_filled
    FROM base
    WHERE opened_date >= DATE '2023-06-25'
)
SELECT
    *,
    AVG(sqft) OVER (PARTITION BY region ORDER BY is_south, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sqft_avg_6_prior,
    ROW_NUMBER() OVER w_ordered AS row_seq,
    SUM(greatest_is_south_sqft_filled) OVER (PARTITION BY region ORDER BY greatest_is_south_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_greatest_is_south_sqft_filled,
    FIRST_VALUE(is_south) IGNORE NULLS OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_south,
    NTILE(3) OVER w_running AS is_south_bucket
FROM base_stores
WINDOW w_ordered AS (PARTITION BY region ORDER BY sqft_filled, id), w_running AS (PARTITION BY region ORDER BY is_south DESC, id)
