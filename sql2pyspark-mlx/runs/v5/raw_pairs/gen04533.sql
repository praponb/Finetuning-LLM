select
    plan,
    start_date,
    customer_id,
    concat(status, ' - ', plan) as status_plan
from subscriptions
where plan = 'standard';
