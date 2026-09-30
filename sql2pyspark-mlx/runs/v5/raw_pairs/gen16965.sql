WITH with_last AS (
    SELECT
        s.id,
        s.supplier_name,
        latest.product_cost
    FROM suppliers s
    LEFT JOIN LATERAL (
            SELECT c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    SELECT
        id,
        product_cost,
        COUNT(product_cost) OVER w_recent AS last4_count_product_cost,
        MIN(product_cost) OVER w_recent AS last4_min_product_cost,
        MAX(product_cost) OVER w_recent AS last4_max_product_cost,
        SUM(product_cost) OVER w_recent AS last4_sum_product_cost
    FROM with_last
    WHERE UPPER(supplier_name) LIKE '%URO%'
    WINDOW w_recent AS (ORDER BY product_cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    SELECT id
    FROM metrics
    INTERSECT ALL
    SELECT id
    FROM suppliers
    WHERE country = 'TH'
),
reconciled AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.id = k.id
),
kept AS (
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.cost BETWEEN 310 AND 570
        )
),
daily AS (
    SELECT
        CASE WHEN last4_count_product_cost >= 15 THEN 'LARGE' ELSE 'SMALL' END AS last4_count_product_cost_tier,
        COUNT(*) AS event_count,
        COUNT(*) AS cnt,
        ROUND(AVG(last4_max_product_cost), 2) AS avg_last4_max_product_cost,
        ROUND(AVG(last4_max_product_cost), 2) AS avg_last4_max_product_cost_2,
        COUNT(DISTINCT id) AS distinct_ids,
        APPROX_PERCENTILE(product_cost, 0.9) AS product_cost_p90
    FROM kept
    GROUP BY 1
)
SELECT
    last4_count_product_cost_tier,
    cnt,
    avg_last4_max_product_cost,
    avg_last4_max_product_cost_2,
    distinct_ids,
    product_cost_p90
FROM daily
ORDER BY product_cost_p90 DESC;
