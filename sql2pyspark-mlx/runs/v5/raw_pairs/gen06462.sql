WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(city) AS city_length,
        CONCAT_WS('/', region, store_name) AS region_store_name_key
    FROM stores
    WHERE region IS NOT NULL
      AND store_name IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        city_length,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS seq_no,
        SUM(city_length) OVER (ORDER BY city_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_city_length,
        DENSE_RANK() OVER w_grp AS city_length_dense_rank
    FROM filtered
    WHERE region = 'West'
    WINDOW w_grp AS (PARTITION BY city ORDER BY city_length DESC NULLS LAST)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.shipping_fee <= 38
        )
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.status,
        top1.amount
    FROM kept s
    JOIN LATERAL (
            SELECT c.status, c.amount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.units > 21
        )
)
SELECT
    id,
    city_length,
    seq_no,
    cumulative_city_length,
    city_length_dense_rank
FROM eligible;
