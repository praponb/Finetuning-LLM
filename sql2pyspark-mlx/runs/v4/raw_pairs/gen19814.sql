WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, DATE_SUB(shipped_date, 20) AS shipped_date_minus_20d
    FROM shipments
    WHERE shipped_date > DATE '2023-03-10'
      AND order_id IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY shipped_date_minus_20d DESC NULLS LAST, id) = 1
),
joined AS (
    -- Attach orders attributes
    SELECT src.*, d.discount
    FROM latest src
    LEFT JOIN orders d
        ON src.order_id = d.id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('YEAR', shipped_date_minus_20d) AS year_start,
        order_id,
        SIZE(COLLECT_SET(carrier)) AS num_carriers,
        MIN(cost) AS min_cost,
        SIZE(COLLECT_SET(carrier)) AS num_carriers_2,
        GROUPING(DATE_TRUNC('YEAR', shipped_date_minus_20d)) AS grouping_level
    FROM joined
    GROUP BY CUBE (DATE_TRUNC('YEAR', shipped_date_minus_20d), order_id)
    HAVING num_carriers_2 >= 1
)
SELECT
    year_start,
    order_id,
    min_cost,
    grouping_level
FROM stats;
