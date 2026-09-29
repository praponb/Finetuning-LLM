SELECT is_active, CASE WHEN country = 'TH' THEN 1 ELSE 0 END AS is_th, CAST(is_active AS INT) AS is_active_flag FROM customers ORDER BY is_th;
