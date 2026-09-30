select year(delivered_date) delivered_year, round((100 * sum(case when status = 'IN_TRANSIT' then 1 else 0 end)) / count(*), 1) pct_in_transit
from shipments
where order_id in (4, 5) and (order_id is null or cost between 71 and 93)
group by delivered_year
having round((100 * sum(case when status = 'IN_TRANSIT' then 1 else 0 end)) / count(*), 1) < 53
order by pct_in_transit desc
