select region, quarter(opened_date) as opened_quarter, round((100 * sum(case when city = 'Chiang Mai' then 1 else 0 end)) / count(*), 1) as pct_chiang_mai
from stores
where region in ('Central', 'West') and city = 'Chiang Mai'
group by region, quarter(opened_date)
having pct_chiang_mai < 68
