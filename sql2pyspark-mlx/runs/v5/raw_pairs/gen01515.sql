select a.id, a.opened_date, a.customer_id from accounts a where a.customer_id in (select id from customers where upper(trim(name)) = 'FRANK MILLER') order by a.opened_date;
