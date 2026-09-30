select s.id, s.started_at from sessions s where s.customer_id not in (select id from customers where upper(trim(name)) = 'KANYA SRISUK');
