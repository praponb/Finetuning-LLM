WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        last_restock,
        DATEDIFF(CURRENT_DATE(), last_restock) days_since_last_restock,
        CASE WHEN quantity >= 240 THEN 'High' WHEN quantity >= 60 THEN 'Medium' ELSE 'Low' END quantity_band
    FROM inventory
    WHERE last_restock >= DATE '2023-06-16'
),
joined AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.launch_date,
        p.category,
        p.product_name
    FROM base src
    LEFT JOIN products p
        ON src.product_id = p.id
    WHERE ARRAY_CONTAINS(p.tags, 'clearance')
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(days_since_last_restock) OVER (PARTITION BY category) category_stddev_samp_days_since_last_restock,
        RANK() OVER (PARTITION BY product_id ORDER BY days_since_last_restock NULLS FIRST) days_since_last_restock_rank,
        LAST_VALUE(days_since_last_restock, TRUE) OVER (ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_days_since_last_restock
    FROM joined
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(days_since_last_restock_rank) OVER w_recent last3_avg_days_since_last_restock_rank,
        MAX(days_since_last_restock_rank) OVER w_recent last3_max_days_since_last_restock_rank,
        COUNT(days_since_last_restock_rank) OVER w_recent last3_count_days_since_last_restock_rank,
        SUM(days_since_last_restock_rank) OVER w_recent last3_sum_days_since_last_restock_rank
    FROM scored
    WINDOW w_recent AS (PARTITION BY category ORDER BY last_restock, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY days_since_last_restock_rank NULLS LAST, id) AS rn
        FROM metrics
    ) ranked
    WHERE rn = 1
)
SELECT
    s.id,
    s.product_id,
    s.days_since_last_restock,
    s.category,
    s.product_name,
    s.category_stddev_samp_days_since_last_restock,
    s.days_since_last_restock_rank,
    s.last3_avg_days_since_last_restock_rank,
    s.last3_max_days_since_last_restock_rank,
    (SELECT SUM(x.quantity) FROM inventory x WHERE x.product_id = s.product_id) AS sum_inventory_quantity
FROM latest s
ORDER BY last3_avg_days_since_last_restock_rank DESC NULLS FIRST;
