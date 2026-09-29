select id, txn_type, ceil(amount) as amount_whole from transactions where (not is_flagged and not is_flagged) or txn_time < '2025-01-01'
