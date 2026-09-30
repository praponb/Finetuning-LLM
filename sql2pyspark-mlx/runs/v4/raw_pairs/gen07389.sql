select id, method, case when status = 'SUCCESS' then 1 else 0 end as is_success, to_date(paid_at) as paid_at_date from payments where order_id is not null order by id desc limit 50
