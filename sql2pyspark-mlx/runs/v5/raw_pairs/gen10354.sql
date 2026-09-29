select city, age, name, coalesce(credit_limit, 0) as credit_limit_filled from customers where country = 'DE' and (is_active = false or segment not in ('Consumer', 'SMB'))
