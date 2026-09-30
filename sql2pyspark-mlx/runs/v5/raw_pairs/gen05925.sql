WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        ROUND(quantity * 0.85, 2) AS adjusted_quantity
    FROM inventory
    WHERE last_restock >= DATE '2023-03-13'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY adjusted_quantity DESC NULLS FIRST, id) AS latest_rank
        FROM cleaned
    ) ranked
    WHERE latest_rank = 1
),
joined AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.quantity,
        s.last_restock,
        s.latest_rank,
        b.opened_date
    FROM latest s
    JOIN stores b
        ON s.store_id = b.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC) AS quantity_dense_rank,
        COUNT(quantity) OVER w_running AS store_id_count_quantity,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY latest_rank DESC, id) AS seq_no,
        MIN(quantity) OVER w_running AS store_id_min_quantity
    FROM joined
    WHERE latest_rank <> 1
    WINDOW w_running AS (PARTITION BY store_id)
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.*,
        s.opened_date AS store_opened_date,
        s.store_name,
        s.city
    FROM enriched a
    LEFT JOIN stores s
        ON a.store_id = s.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id_min_quantity DESC NULLS FIRST, id) = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN quantity_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_dense_rank_tier,
    DATE_TRUNC('YEAR', last_restock) AS year_start,
    CASE WHEN store_id_count_quantity >= 13 THEN 'HIGH' ELSE 'LOW' END AS store_id_count_quantity_tier,
    MAX(quantity_dense_rank) FILTER (WHERE city = 'Bangkok') AS bangkok_max_quantity_dense_rank,
    APPROX_PERCENTILE(quantity, 0.5) AS quantity_p50,
    PERCENTILE_APPROX(seq_no, 0.95) AS seq_no_p95,
    SUM(CASE WHEN city = 'Chiang Mai' THEN store_id_count_quantity ELSE 0 END) AS chiang_mai_store_id_count_quantity,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(DISTINCT city) AS distinct_cities
FROM ranked
GROUP BY CASE WHEN quantity_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('YEAR', last_restock), CASE WHEN store_id_count_quantity >= 13 THEN 'HIGH' ELSE 'LOW' END
ORDER BY quantity_p50 DESC NULLS LAST
