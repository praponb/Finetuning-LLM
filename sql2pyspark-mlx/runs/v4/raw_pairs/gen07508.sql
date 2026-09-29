SELECT
    CASE WHEN YEAR(txn_time) < 2024 THEN 'before_2024' WHEN YEAR(txn_time) = 2024 THEN '2024' ELSE 'after_2024' END AS txn_cohort,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(txn_type))) AS txn_type_values,
    MAX(txn_time) AS last_txn_time,
    COUNT(DISTINCT amount) AS unique_txn_types
FROM transactions
GROUP BY 1
