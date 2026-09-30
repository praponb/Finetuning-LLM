select status from payments where order_id is not null union all select status from payments where status = 'FAILED';
