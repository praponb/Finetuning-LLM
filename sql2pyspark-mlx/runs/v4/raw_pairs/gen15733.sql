select status, order_id from payments where amount is not null union all select status, order_id from payments where amount between 670 and 920 order by status
