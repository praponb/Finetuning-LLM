WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        customer_id,
        items,
        attributes,
        COALESCE(ship_to.country, 'UNKNOWN') AS country_or_unknown,
        SIZE(attributes) AS num_attributes
    FROM carts
)
-- Window metrics over several different partitions
SELECT
    *,
    COUNT(num_attributes) OVER (PARTITION BY customer_id) AS customer_id_count_num_attributes,
    LEAD(num_attributes, 2) OVER w_part AS next_num_attributes,
    ROW_NUMBER() OVER w_running AS row_seq
FROM unpacked
WINDOW w_part AS (ORDER BY num_attributes, id), w_running AS (PARTITION BY customer_id ORDER BY num_attributes, id);
