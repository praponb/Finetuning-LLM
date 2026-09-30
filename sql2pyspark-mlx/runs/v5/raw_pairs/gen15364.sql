select c.signup_date, c.credit_limit, o.shipping_fee from customers c left outer join orders o on o.customer_id = c.id;
