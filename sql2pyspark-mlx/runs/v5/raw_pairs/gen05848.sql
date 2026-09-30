select a.id, a.account_type, c.city, c.is_active from accounts a join customers c on a.customer_id = c.id where c.city in ('Singapore', 'Tokyo') or a.opened_date between '2024-03-01' and '2025-08-01'
