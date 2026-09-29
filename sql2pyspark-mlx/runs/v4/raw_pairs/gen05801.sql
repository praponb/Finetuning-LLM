select *, rank() over (partition by plan order by start_date) as rank_in_group
from subscriptions
qualify rank_in_group = 1
