select status, sum(case when method = 'promptpay' then 1 else 0 end) promptpay_count, min(paid_at) min_paid_at from payments where amount < 1070 group by status order by status
