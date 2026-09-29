select *, if(is_active, 'Yes', 'No') as is_active_label from customers order by signup_date desc
