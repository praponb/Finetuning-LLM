WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(quantity, 0) AS quantity_rounded
    FROM order_items
    WHERE product_id IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_hist AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS event_seq_2,
        FIRST_VALUE(quantity_rounded) IGNORE NULLS OVER (ORDER BY quantity_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_rounded,
        SUM(quantity_rounded) OVER (PARTITION BY order_id, product_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity_rounded
    FROM base
    WINDOW w_hist AS (ORDER BY quantity DESC, id)
),
enriched AS (
    -- Attach orders attributes
    SELECT a.*, b.order_date
    FROM scored a
    LEFT JOIN orders b
        ON a.order_id = b.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT e.*, o.amount
    FROM enriched e
    LEFT JOIN orders o
        ON e.order_id = o.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_dims
    INTERSECT ALL
    SELECT id
    FROM order_items
    WHERE unit_price > 330
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN matched_keys k ON s.id = k.id
),
matched_keys_2 AS (
    -- Set difference on keys
    SELECT id, product_id
    FROM kept_rows
    EXCEPT
    SELECT id, product_id
    FROM order_items
    WHERE unit_price <= 240
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    JOIN matched_keys_2 k ON s.id = k.id AND s.product_id = k.product_id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        product_id,
        CASE WHEN event_seq_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_2_tier,
        BIT_AND(quantity) AS quantity_bit_and,
        MIN(event_seq) AS min_event_seq,
        COUNT(DISTINCT order_id) AS distinct_order_ids,
        MIN(first_known_quantity_rounded) AS min_first_known_quantity_rounded,
        COUNT(DISTINCT order_id) AS distinct_order_ids_2
    FROM reconciled
    GROUP BY product_id, event_seq_2_tier
)
SELECT
    product_id,
    event_seq_2_tier,
    min_event_seq,
    min_first_known_quantity_rounded
FROM grouped;
