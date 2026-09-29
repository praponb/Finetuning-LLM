select start_date
from subscriptions
where year(end_date) >= 2024 or monthly_fee < 70
order by start_date
