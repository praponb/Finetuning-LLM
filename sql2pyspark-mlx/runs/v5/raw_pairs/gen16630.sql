select status, count(case when method = 'paypal' then 1 end) as paypal_count, round((100 * sum(case when method = 'card' then 1 else 0 end)) / count(*), 1) as pct_card
from payments
group by status
order by 3
