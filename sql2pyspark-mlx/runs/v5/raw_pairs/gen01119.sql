SELECT account_type, status, balance, CONCAT_WS(',', account_type, status) AS account_type_status_key FROM accounts WHERE balance > 48000 AND balance >= 24000 AND account_type = 'SAVINGS';
