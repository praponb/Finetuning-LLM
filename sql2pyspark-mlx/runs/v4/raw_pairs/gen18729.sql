select email, segment, city, case when is_active then 'Yes' else 'No' end as is_active_label from customers where email like '%@startup.io';
