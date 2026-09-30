select channel, id
from promotions
except distinct
select channel, id
from promotions
where discount_pct < 0.28;
