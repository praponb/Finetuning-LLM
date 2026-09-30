WITH base AS (
    SELECT *, CAST(quantity AS STRING) quantity_num
    FROM order_items
    WHERE product_id IS NOT NULL
      AND product_id IS NOT NULL
),
filtered AS (
    SELECT
        *,
        unit_price + quantity line_total,
        CAST(quantity AS DOUBLE) quantity_num_2
    FROM base
    WHERE product_id IS NOT NULL
      AND quantity BETWEEN 5 AND 7
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY line_total DESC, id ASC) AS row_num
    FROM filtered
    QUALIFY row_num = 1
),
metrics AS (
    SELECT
        id,
        order_id,
        quantity,
        quantity_num,
        line_total,
        quantity_num_2,
        ROW_NUMBER() OVER w_running seq_no,
        FIRST_VALUE(unit_price, TRUE) OVER w_running_2 first_known_unit_price,
        SUM(unit_price) OVER w_hist cumulative_unit_price
    FROM latest
    WHERE quantity_num_2 BETWEEN 45 AND 77
    WINDOW w_running AS (PARTITION BY order_id, product_id ORDER BY line_total ASC, id ASC), w_running_2 AS (PARTITION BY order_id ORDER BY line_total ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (ORDER BY line_total ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    SELECT a.*, o.order_date
    FROM metrics a
    LEFT JOIN orders o
        ON a.order_id = o.id
),
latest_order_items AS (
    SELECT *
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity_num_2 ASC, id ASC) = 1
),
latest_order_items_2 AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity_num_2 DESC, id ASC) AS row_num
    FROM latest_order_items
    QUALIFY row_num = 1
)
SELECT
    id,
    order_id,
    quantity,
    line_total,
    quantity_num_2,
    first_known_unit_price,
    order_date,
    row_num
FROM latest_order_items_2
ORDER BY order_date DESC;
