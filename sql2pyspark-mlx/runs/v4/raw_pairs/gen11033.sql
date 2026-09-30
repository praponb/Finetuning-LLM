select end_date, trim(plan) as clean_plan
from subscriptions
where year(end_date) >= 2025;
