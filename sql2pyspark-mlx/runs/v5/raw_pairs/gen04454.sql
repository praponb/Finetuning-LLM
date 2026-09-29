WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        CASE WHEN rating >= 4.5 THEN 'High' WHEN rating >= 1.9 THEN 'Medium' ELSE 'Low' END AS rating_band
    FROM suppliers
    WHERE city IS NOT NULL
      AND country IN ('US', 'SG')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY country, city ORDER BY rating ASC, id ASC) AS rating_bucket,
        NTILE(10) OVER (ORDER BY rating DESC, id ASC) AS rating_bucket_2,
        FIRST_VALUE(rating) IGNORE NULLS OVER (PARTITION BY city, country ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating
    FROM cleaned
    WHERE city NOT IN ('Berlin', 'London', 'Chiang Mai')
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.brand NOT IN ('Acme', 'Initech', 'Stark')
        )
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        city,
        rating,
        rating_bucket_2,
        first_known_rating
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY rating_bucket_2 DESC NULLS LAST) AS latest_rank
        FROM eligible
    ) ranked
    WHERE latest_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        rating,
        rating_bucket_2,
        AVG(first_known_rating) OVER (PARTITION BY city ORDER BY rating_bucket_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_first_known_rating,
        MIN(first_known_rating) OVER (PARTITION BY city ORDER BY rating_bucket_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_first_known_rating,
        SUM(first_known_rating) OVER (PARTITION BY city ORDER BY rating_bucket_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_first_known_rating
    FROM deduped
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.launch_date,
        top1.stock_qty
    FROM scored s
    JOIN LATERAL (
            SELECT c.launch_date, c.stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        city,
        stock_qty
    FROM with_latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY running_min_first_known_rating DESC, id ASC) = 1
)
SELECT id
FROM latest
