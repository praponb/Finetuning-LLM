select balance, account_type, coalesce(status, 'unassigned') status_filled from accounts
