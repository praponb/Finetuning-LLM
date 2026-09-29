WITH enriched AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.last_restock,
        d.opened_date
    FROM inventory s
    LEFT JOIN stores d
        ON s.store_id = d.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC NULLS LAST, id) AS dedup_rank
        FROM enriched
    ) ranked
    WHERE dedup_rank = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        product_id,
        last_restock
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY last_restock DESC NULLS FIRST, id) AS dedup_rank_2
        FROM deduped
    ) ranked
    WHERE dedup_rank_2 = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LAST_DAY(last_restock) AS last_restock_month_end
    FROM ranked
    WHERE last_restock >= TO_DATE('2023-04-11')
      AND YEAR(last_restock) = 2024
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, product_id
    FROM cleaned
    INTERSECT
    SELECT id, product_id
    FROM inventory
    WHERE quantity = 185
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.product_id = k.product_id
),
joined AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.last_restock,
        b.brand,
        b.cost,
        b.category
    FROM verified e
    LEFT JOIN products b
        ON e.product_id = b.id
    WHERE b.cost < 520
),
scored AS (
    -- Ranking and gap features
    SELECT *, CUME_DIST() OVER (ORDER BY cost DESC) AS cost_cume_dist
    FROM joined
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', last_restock) AS week_start,
    CASE WHEN cost >= 690 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    PERCENTILE_APPROX(cost_cume_dist, 0.5) AS cost_cume_dist_p50,
    AVG(cost) AS avg_cost,
    GROUPING_ID() AS gid
FROM scored
GROUP BY ROLLUP (DATE_TRUNC('WEEK', last_restock), CASE WHEN cost >= 690 THEN 'LARGE' ELSE 'SMALL' END);
