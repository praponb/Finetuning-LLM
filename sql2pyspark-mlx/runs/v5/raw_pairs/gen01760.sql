select full_name, lower(level) as level_lower, coalesce(level, 'N/A') as level_filled from employees where full_name not like 'Da%'
