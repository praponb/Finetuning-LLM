select
    case when quantity >= 211 then 'high' when quantity >= 88 then 'mid' else 'low' end as quantity_bucket,
    store_id,
    round(stddev(quantity), 2) as stddev_quantity,
    avg(quantity) as avg_quantity,
    count(quantity) as quantity_count
from inventory
where store_id in (1, 4, 5)
group by quantity_bucket, store_id
order by quantity_count desc;
