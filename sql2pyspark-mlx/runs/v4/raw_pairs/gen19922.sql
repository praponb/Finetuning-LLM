select status, max(balance) - min(balance) balance_range from accounts group by status
