select *, case when plan = 'premium' then 1 else 0 end as is_premium
from subscriptions
where plan in ('standard', 'enterprise', 'basic');
