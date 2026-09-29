select signup_date, 'customer' source from customers where name not like 'Pim%' union all select opened_date, 'account' source from accounts where balance > 78000
