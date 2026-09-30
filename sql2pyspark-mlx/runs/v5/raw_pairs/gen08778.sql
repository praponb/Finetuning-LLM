WITH filtered AS (
    SELECT
        id,
        store_id,
        last_restock,
        DATE_TRUNC('WEEK', last_restock) AS last_restock_period,
        DATE_FORMAT(last_restock, 'MMM yyyy') AS last_restock_label
    FROM inventory
    WHERE last_restock > DATE '2023-08-16'
      AND store_id = 6
)
SELECT
    s.*,
    d.store_name,
    d.city
FROM filtered s
LEFT JOIN stores d
    ON s.store_id = d.id;
