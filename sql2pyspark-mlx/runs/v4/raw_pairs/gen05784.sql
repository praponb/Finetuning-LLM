WITH valid AS (
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        unit_price * quantity AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND quantity >= 5
),
with_dims AS (
    SELECT
        e.*,
        d.amount,
        d.order_date
    FROM valid e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY amount DESC) AS rn
    FROM with_dims
    QUALIFY rn = 1
),
grouped AS (
    SELECT
        CASE WHEN line_total >= 87 THEN 'LARGE' ELSE 'SMALL' END AS line_total_tier,
        DATE_TRUNC('YEAR', order_date) AS year_start,
        order_id,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(quantity, 0.95) AS quantity_p95,
        CORR(quantity, unit_price) AS corr_quantity_unit_price,
        APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles
    FROM ranked
    GROUP BY CASE WHEN line_total >= 87 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', order_date), order_id
    HAVING quantity_p95 >= 0
)
SELECT
    s.line_total_tier,
    s.year_start,
    s.order_id,
    s.distinct_ids,
    s.quantity_p95,
    s.corr_quantity_unit_price,
    (SELECT SUM(s2.cost) FROM shipments s2 WHERE s2.order_id = s.order_id) AS sum_shipment_cost
FROM grouped s
ORDER BY order_id DESC NULLS LAST
