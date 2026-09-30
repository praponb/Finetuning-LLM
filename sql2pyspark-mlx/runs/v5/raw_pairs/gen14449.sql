SELECT *, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label, ROUND(amount * 1.07, 2) AS amount_adjusted
FROM transactions;
