select merchant, is_flagged, round(avg(amount), 2) as avg_amount
from transactions
where merchant not in ('Shell', 'Grab')
group by 1, 2
order by merchant
