select c.id, c.country
from customers c
where c.is_active = true and exists (select 1 from accounts a where a.customer_id = c.id and a.opened_date <= '2025-01-01')
