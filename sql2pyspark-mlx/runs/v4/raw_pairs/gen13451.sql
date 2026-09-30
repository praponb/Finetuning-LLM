select
    id,
    customer_id,
    monthly_fee,
    upper(status) as status_upper
from subscriptions
where end_date <= '2025-03-01' and start_date between '2023-07-01' and '2025-01-08';
