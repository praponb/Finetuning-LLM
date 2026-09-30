select status, order_id from payments where amount between 480 and 1370 union all select status, order_id from payments where amount >= 1380
