select status, carrier, round(stddev(cost), 2) as stddev_cost
from shipments
where order_id is not null
group by status, carrier
having sum(case when carrier = 'FedEx' then cost else 0 end) > 49
order by stddev_cost desc
