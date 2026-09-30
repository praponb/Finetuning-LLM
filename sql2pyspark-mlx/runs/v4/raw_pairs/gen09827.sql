WITH cleaned AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        ROUND(quantity / unit_price, 2) AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND quantity <= 5
),
valid AS (
    SELECT
        *,
        ROUND(quantity * 1.07, 2) AS adjusted_quantity,
        ROUND(quantity, 1) AS quantity_rounded
    FROM cleaned
    WHERE product_id IS NOT NULL
),
cleaned_order_items AS (
    SELECT
        id,
        order_id,
        product_id,
        line_total,
        adjusted_quantity,
        quantity_rounded,
        ROUND(adjusted_quantity * 0.9, 2) AS adjusted_quantity_adjusted,
        CAST(unit_price AS INT) AS unit_price_int
    FROM valid
    WHERE product_id IS NOT NULL
      AND line_total > 49
),
enriched AS (
    SELECT
        src.*,
        b.stock_qty,
        b.brand
    FROM cleaned_order_items src
    LEFT JOIN products b
        ON src.product_id = b.id
)
SELECT
    *,
    NTILE(3) OVER (ORDER BY quantity_rounded DESC, id ASC) AS quantity_rounded_bucket,
    NTILE(3) OVER w_grp AS adjusted_quantity_bucket,
    ROW_NUMBER() OVER w_grp AS row_seq,
    SUM(stock_qty) OVER (PARTITION BY product_id) AS product_id_sum_stock_qty
FROM enriched
WINDOW w_grp AS (PARTITION BY order_id ORDER BY adjusted_quantity DESC, id ASC);
