SELECT id, amount, CONCAT_WS(',', txn_type, merchant) AS txn_type_merchant_key FROM transactions WHERE is_flagged;
