SELECT
    id,
    txn_type,
    IF(is_flagged, 'Yes', 'No') is_flagged_label,
    COALESCE(amount, 0) amount_filled
FROM transactions
WHERE TO_DATE(txn_time) < '2024-07-01' AND (amount >= 1000 OR merchant = 'Grab');
