select id, country, email, case when is_active then 'Yes' else 'No' end as is_active_label from customers where name like 'Gra%' and age < 36 and is_active
