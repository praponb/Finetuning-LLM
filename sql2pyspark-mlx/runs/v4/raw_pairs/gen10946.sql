WITH joined AS (
    SELECT
        s.*,
        d.order_date,
        d.discount
    FROM order_items s
    LEFT JOIN orders d
        ON s.order_id = d.id
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY unit_price NULLS LAST, id) AS latest_rank
    FROM joined
    QUALIFY latest_rank = 1
),
with_windows AS (
    SELECT
        *,
        AVG(unit_price) OVER w_ordered order_id_avg_unit_price,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) unit_price_bucket,
        MAX(discount) OVER w_ordered order_id_max_discount
    FROM latest
    WINDOW w_ordered AS (PARTITION BY order_id)
)
SELECT *
FROM (
    SELECT
        id,
        order_id,
        unit_price,
        discount
    FROM with_windows
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric_name IN (unit_price, discount)
);
