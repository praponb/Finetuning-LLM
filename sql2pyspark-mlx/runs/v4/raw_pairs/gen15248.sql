SELECT id, segment, IF(is_active, 'Yes', 'No') AS is_active_label FROM customers WHERE age <> 43;
