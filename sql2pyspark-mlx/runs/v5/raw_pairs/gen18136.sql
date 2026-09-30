WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS attr_key, v
)
SELECT
    id,
    attr_key,
    v,
    FIRST_VALUE(attr_key) IGNORE NULLS OVER w_part AS first_known_attr_key,
    ROW_NUMBER() OVER w_grp AS seq_no,
    ROW_NUMBER() OVER w_grp AS row_seq
FROM flattened
WINDOW w_part AS (PARTITION BY attr_key ORDER BY created_at, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY customer_id ORDER BY created_at, id, attr_key)
