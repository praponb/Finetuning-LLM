WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        DATE_FORMAT(last_restock, 'yyyy-MM-dd') last_restock_label
    FROM inventory
    WHERE last_restock >= DATE '2023-08-20'
      AND product_id IN (6, 10)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST, id) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
joined AS (
    -- Attach stores attributes
    SELECT
        s.*,
        d.store_name,
        d.sqft
    FROM latest s
    JOIN stores d
        ON s.store_id = d.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(quantity) OVER (PARTITION BY product_id ORDER BY latest_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_avg_quantity,
        MIN(quantity) OVER (PARTITION BY product_id ORDER BY latest_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_min_quantity,
        COUNT(quantity) OVER (PARTITION BY product_id ORDER BY latest_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_count_quantity,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY latest_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_sum_quantity,
        RANK() OVER (ORDER BY sqft DESC NULLS FIRST) sqft_rank
    FROM joined
    WHERE latest_rank < 1
)
-- Join a dimension and aggregate
SELECT
    p.city,
    COUNT(*) AS cnt,
    MAX(e.last3_min_quantity) AS max_last3_min_quantity,
    GROUPING_ID() AS gid
FROM scored e
JOIN stores p ON e.store_id = p.id
GROUP BY CUBE (p.city)
ORDER BY max_last3_min_quantity NULLS FIRST;
