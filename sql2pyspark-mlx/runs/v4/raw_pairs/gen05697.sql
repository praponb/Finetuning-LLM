WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN quantity >= 8 THEN 'High' WHEN quantity >= 4 THEN 'Medium' ELSE 'Low' END AS quantity_band
    FROM order_items
    WHERE order_id = 33
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity_band,
        unit_price * quantity AS line_total,
        unit_price + quantity AS line_total_2
    FROM valid
    WHERE product_id IS NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(line_total_2) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_line_total_2,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY line_total_2 NULLS LAST) AS line_total_2_cume_dist
    FROM cleaned
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY line_total_2 DESC, id) AS dedup_rank
        FROM scored
    ) x
    WHERE dedup_rank = 1
),
joined AS (
    -- Attach products attributes
    SELECT
        s.*,
        p.product_name,
        p.stock_qty
    FROM latest s
    LEFT JOIN products p
        ON s.product_id = p.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT product_id, id
    FROM joined
    EXCEPT DISTINCT
    SELECT product_id, id
    FROM order_items
    WHERE order_id = 40
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN kept_keys k ON s.product_id = k.product_id AND s.id = k.id
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT product_id, order_id
    FROM reconciled
    INTERSECT ALL
    SELECT product_id, order_id
    FROM order_items
    WHERE quantity < 5
),
reconciled_order_items AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id AND s.order_id = k.order_id
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.quantity,
    s.line_total,
    s.line_total_2,
    s.last_known_line_total_2,
    s.line_total_2_cume_dist,
    s.dedup_rank,
    (SELECT MAX(x.quantity) FROM order_items x WHERE x.product_id = s.product_id) AS max_order_item_quantity
FROM reconciled_order_items s
ORDER BY dedup_rank DESC, last_known_line_total_2
