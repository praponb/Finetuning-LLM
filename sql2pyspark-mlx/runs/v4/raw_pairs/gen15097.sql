SELECT
    id,
    is_active,
    segment,
    CAST(is_active AS INT) AS is_active_flag
FROM customers
ORDER BY is_active_flag DESC
