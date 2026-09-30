SELECT is_flagged, CAST(txn_time AS DATE) AS txn_time_day, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label FROM transactions WHERE account_id = 19 ORDER BY txn_time_day;
