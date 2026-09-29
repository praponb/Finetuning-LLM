WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.product_id = 1
        )
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.store_sale_units
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.units AS store_sale_units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(store_sale_units, 1) OVER (PARTITION BY city ORDER BY store_sale_units, id) AS prev_store_sale_units,
        SUM(store_sale_units) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_store_sale_units
    FROM latest_match
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER w_ordered AS store_sale_units_bucket,
        SUM(cumulative_store_sale_units) OVER w_hist AS running_cumulative_store_sale_units
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY city ORDER BY store_sale_units DESC, id), w_hist AS (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    store_name,
    region,
    sqft,
    cumulative_store_sale_units,
    store_sale_units_bucket,
    CASE WHEN store_name RLIKE '^(Riverside|Old Town)$' THEN 'CORE' WHEN store_name RLIKE '^(Siam Flagship|Downtown Express)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS store_name_zone
FROM scored
ORDER BY store_name_zone NULLS LAST;
