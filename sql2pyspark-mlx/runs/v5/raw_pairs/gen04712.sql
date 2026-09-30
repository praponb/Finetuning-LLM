select *, date_format(end_date, 'yyyy-MM-dd') as end_date_label, concat_ws('-', status, plan) as status_plan_key
from subscriptions
where year(end_date) >= 2025
