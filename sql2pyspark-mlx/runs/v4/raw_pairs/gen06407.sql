SELECT TO_DATE(paid_at) AS paid_day, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) AS method_values FROM payments WHERE method IN ('promptpay', 'card', 'bank_transfer') GROUP BY TO_DATE(paid_at);
