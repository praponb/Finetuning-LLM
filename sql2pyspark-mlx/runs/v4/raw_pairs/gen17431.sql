select s.*, c.credit_limit from sessions s inner join customers c on s.customer_id = c.id;
