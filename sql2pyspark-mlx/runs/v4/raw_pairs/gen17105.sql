select id, paid_at, floor(amount) as amount_whole from payments where amount < 630 and method = 'cash'
