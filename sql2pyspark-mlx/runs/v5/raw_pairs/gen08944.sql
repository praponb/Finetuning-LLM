select opened_date, customer_id, status, initcap(trim(account_type)) as account_type_title from accounts where opened_date < '2024-11-04' order by customer_id desc;
