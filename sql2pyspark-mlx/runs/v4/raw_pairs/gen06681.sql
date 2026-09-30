SELECT id, is_flagged, CEIL(amount) AS amount_whole FROM transactions WHERE merchant = 'Amazon' AND txn_type NOT IN ('CREDIT', 'REFUND', 'FEE') ORDER BY amount_whole DESC
