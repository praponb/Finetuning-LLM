SELECT CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END paid_cohort, CASE WHEN amount >= 1670 THEN 'high' WHEN amount >= 710 THEN 'mid' ELSE 'low' END amount_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) method_values
FROM payments
GROUP BY paid_cohort, amount_bucket
