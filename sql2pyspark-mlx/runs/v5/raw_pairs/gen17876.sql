WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS attr_key, v
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM flattened
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, attr_key ORDER BY created_at, id, attr_key) = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY created_at) AS dedup_rank
    FROM deduped
    QUALIFY dedup_rank = 1
),
latest_carts AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        created_at,
        attr_key,
        v,
        dedup_rank
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY dedup_rank DESC, id, attr_key) = 1
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.attr_key,
        d.city
    FROM latest_carts a
    INNER JOIN customers d
        ON a.customer_id = d.id
)
-- Grouped metrics
SELECT
    customer_id,
    attr_key,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE attr_key = 'gift') AS gift_rows,
    COUNT(*) FILTER (WHERE city = 'London') AS london_rows,
    COUNT(*) FILTER (WHERE attr_key = 'weight_kg') AS weight_kg_rows,
    COUNT(*) AS cnt
FROM with_dims
GROUP BY 1, 2
ORDER BY cnt
