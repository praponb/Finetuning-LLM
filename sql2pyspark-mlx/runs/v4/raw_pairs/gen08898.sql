select status from payments intersect all select status from payments where status not in ('REFUNDED', 'SUCCESS')
