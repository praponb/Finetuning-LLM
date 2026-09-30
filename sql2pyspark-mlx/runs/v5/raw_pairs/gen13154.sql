WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date >= s.opened_date
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY opened_date DESC, id) AS row_seq,
        LAG(sqft, 1) OVER (PARTITION BY region ORDER BY opened_date, id) AS prev_sqft,
        SUM(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft
    FROM not_flagged
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        WEEKOFYEAR(opened_date) AS opened_date_weekofyear,
        COALESCE(region, 'OTHER') AS resolved_region
    FROM enriched
    WHERE opened_date > DATE '2023-02-19'
      AND region IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(opened_date_weekofyear) OVER (ORDER BY prev_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_opened_date_weekofyear,
        ROW_NUMBER() OVER (ORDER BY sqft, id) AS row_seq_2
    FROM base
    WHERE opened_date_weekofyear <> 77
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_name,
        city,
        sqft,
        running_sqft,
        opened_date_weekofyear,
        resolved_region,
        cumulative_opened_date_weekofyear,
        row_seq_2
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY running_sqft DESC, id) = 1
)
-- Running and moving aggregates
SELECT
    id,
    store_name,
    city,
    running_sqft,
    resolved_region,
    row_seq_2,
    LEAD(sqft, 1) OVER (PARTITION BY city ORDER BY sqft, id) AS next_sqft,
    AVG(running_sqft) OVER (ORDER BY row_seq_2, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS running_sqft_avg_3,
    NTILE(3) OVER (PARTITION BY city ORDER BY running_sqft, id) AS running_sqft_bucket
FROM deduped;
