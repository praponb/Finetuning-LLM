select status, sum(case when status = 'OPEN' then balance else 0 end) as open_balance, max(opened_date) as latest_opened_date from accounts group by status
