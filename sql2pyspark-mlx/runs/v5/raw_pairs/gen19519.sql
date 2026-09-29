select city, year(opened_date) as opened_year, round((100 * sum(case when city = 'Singapore' then 1 else 0 end)) / count(*), 1) as pct_singapore
from stores
group by city, year(opened_date)
order by opened_year, pct_singapore desc
