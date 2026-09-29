SELECT id, amount, merchant, CONCAT_WS('-', merchant, txn_type) AS merchant_txn_type_key FROM transactions ORDER BY merchant;
