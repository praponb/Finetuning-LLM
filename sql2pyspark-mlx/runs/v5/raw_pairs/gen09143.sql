SELECT id, is_flagged, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
FROM transactions
WHERE NOT (account_id IN (4, 30) AND merchant = 'Amazon')
ORDER BY id DESC
