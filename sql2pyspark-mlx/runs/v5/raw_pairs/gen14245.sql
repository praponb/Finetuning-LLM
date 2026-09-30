WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        last_restock,
        QUARTER(last_restock) AS last_restock_quarter
    FROM inventory
    WHERE last_restock > DATE '2023-08-07'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT id, product_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_quarter DESC NULLS FIRST, id) AS rank_in_group
        FROM cleaned
    ) ranked
    WHERE rank_in_group = 1
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        p.cost
    FROM latest s
    LEFT JOIN products p
        ON s.product_id = p.id
    WHERE p.brand IS NOT NULL
),
enriched_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(cost) OVER (PARTITION BY product_id ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_cost,
        SUM(cost) OVER (PARTITION BY product_id ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_cost,
        MAX(cost) OVER (PARTITION BY product_id ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_cost,
        MIN(cost) OVER (PARTITION BY product_id ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_cost
    FROM enriched
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(last3_min_cost) OVER (ORDER BY last3_sum_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last3_min_cost,
        ROUND(last3_max_cost / SUM(last3_max_cost) OVER (PARTITION BY product_id), 4) AS last3_max_cost_share,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY last3_min_cost DESC NULLS FIRST) AS last3_min_cost_cume_dist
    FROM enriched_inventory
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        product_id,
        last3_avg_cost,
        last3_min_cost_cume_dist,
        last3_min_cost
    FROM scored
) src
UNPIVOT INCLUDE NULLS (
    value FOR kpi IN (last3_avg_cost AS LAST3_AVG_COST_VAL, last3_min_cost_cume_dist AS LAST3_MIN_COST_CUME_DIST, last3_min_cost AS LAST3_MIN_COST_VAL)
) u
