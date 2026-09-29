select
    id,
    monthly_fee,
    status,
    upper(status) as status_upper,
    initcap(trim(plan)) as plan_title
from subscriptions;
