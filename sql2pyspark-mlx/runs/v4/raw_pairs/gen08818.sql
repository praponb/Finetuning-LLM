select id, customer_id
from sessions
where started_at < '2025-06-01' or device not in ('mobile', 'tablet');
