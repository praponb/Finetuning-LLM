WITH cleaned AS (
    SELECT
        *,
        CASE WHEN merchant = 'Grab' THEN 1 ELSE 0 END AS is_grab,
        LENGTH(txn_type) AS txn_type_length
    FROM transactions
    WHERE txn_time >= '2024-03-01'
)
SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY txn_type ORDER BY txn_type_length DESC) AS rank_in_group
    FROM cleaned
) x
WHERE rank_in_group = 1;
