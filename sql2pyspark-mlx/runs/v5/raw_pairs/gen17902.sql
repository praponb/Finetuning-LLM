select
    case when quantity >= 245 then 'high' when quantity >= 102 then 'mid' else 'low' end as quantity_bucket,
    round(avg(quantity), 2) as avg_quantity,
    round(stddev(quantity), 2) as stddev_quantity,
    count(quantity) as quantity_count
from inventory
group by case when quantity >= 245 then 'high' when quantity >= 102 then 'mid' else 'low' end
order by avg_quantity desc, stddev_quantity, quantity_bucket
limit 10;
