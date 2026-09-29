SELECT
    status,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    MAX(paid_at) AS ended_at,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    SIZE(COLLECT_SET(status)) AS num_status,
    MAX(amount) FILTER (WHERE status = 'SUCCESS') AS success_max_amount,
    COUNT(DISTINCT status) AS distinct_status
FROM payments
GROUP BY 1
