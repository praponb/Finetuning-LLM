select
    id,
    customer_id,
    opened_date,
    round(balance * 0.9, 2) as balance_scaled
from accounts;
