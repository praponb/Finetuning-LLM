select
    store_id,
    sum(quantity) as total_quantity,
    count(*) as cnt,
    round(sum(quantity) * 100.0 / sum(sum(quantity)) over (), 2) as pct_of_total
from inventory
group by store_id
order by pct_of_total desc, total_quantity
