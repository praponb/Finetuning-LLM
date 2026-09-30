select
    id,
    customer_id,
    status,
    opened_date,
    lower(account_type) as account_type_lower
from accounts
where (opened_date >= '2025-05-01' and balance >= 34000) or opened_date < '2024-11-15'
