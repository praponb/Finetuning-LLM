select status, coalesce(balance, 0) balance_filled from accounts where status is not null
