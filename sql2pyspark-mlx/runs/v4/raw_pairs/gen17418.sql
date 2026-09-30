select status from payments where paid_at < '2023-04-01' union select status from payments where amount > 1420
