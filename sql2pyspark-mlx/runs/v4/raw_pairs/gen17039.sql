select amount, method, status from payments where not (status in ('REFUNDED', 'SUCCESS') or method <> 'card')
