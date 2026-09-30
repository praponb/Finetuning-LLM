with kept_keys as (
    select product_id, id
    from inventory
    except
    select product_id, id
    from inventory
    where last_restock > '2023-09-01'
),
reconciled as (
    select s.*
    from inventory s
    inner join kept_keys k on s.product_id = k.product_id and s.id = k.id
)
select
    date_trunc('month', last_restock) as activity_month,
    case when quantity >= 250 then 'PREMIUM' else 'STANDARD' end as quantity_tier,
    date_trunc('year', last_restock) as year_start,
    round(avg(quantity), 2) as avg_quantity,
    approx_percentile(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles,
    round(avg(quantity), 2) as avg_quantity_2
from reconciled
group by grouping sets ((date_trunc('month', last_restock), case when quantity >= 250 then 'PREMIUM' else 'STANDARD' end, date_trunc('year', last_restock)), (date_trunc('month', last_restock), case when quantity >= 250 then 'PREMIUM' else 'STANDARD' end), (date_trunc('month', last_restock)), ())
having round(avg(quantity), 2) > 0
