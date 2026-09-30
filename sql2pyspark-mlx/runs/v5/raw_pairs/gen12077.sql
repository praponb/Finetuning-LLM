SELECT id, account_id, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label, LOWER(merchant) AS merchant_lower FROM transactions WHERE is_flagged ORDER BY id DESC;
