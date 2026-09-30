with filtered as (
    -- Keep recent, valid rows
    select *, last_day(last_restock) as last_restock_month_end
    from inventory
    where last_restock > date '2023-04-19'
      and product_id in (1, 2)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from filtered
    qualify row_number() over (partition by product_id order by quantity desc, id) = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        min(quantity) over (partition by store_id order by last_restock, id rows between unbounded preceding and current row) as running_min_quantity,
        avg(quantity) over (partition by store_id order by last_restock, id rows between unbounded preceding and current row) as running_avg_quantity,
        count(quantity) over (partition by store_id order by last_restock, id rows between unbounded preceding and current row) as running_count_quantity,
        sum(quantity) over (partition by store_id order by last_restock, id rows between unbounded preceding and current row) as running_sum_quantity
    from latest
    where last_restock > '2023-08-01'
),
joined as (
    -- Attach products attributes
    select
        a.*,
        b.launch_date,
        b.stock_qty,
        b.category
    from enriched a
    left join products b
        on a.product_id = b.id
),
with_dims as (
    -- Attach stores attributes
    select
        a.id,
        a.store_id,
        a.quantity,
        a.last_restock_month_end,
        a.running_sum_quantity,
        a.launch_date,
        s.sqft
    from joined a
    inner join stores s
        on a.store_id = s.id
)
-- Aggregate with conditional metrics
select
    case when running_sum_quantity >= 1010 then 'LARGE' else 'SMALL' end as running_sum_quantity_tier,
    store_id,
    date_trunc('month', launch_date) as activity_month,
    bit_and(quantity) as quantity_bit_and,
    count(distinct id) as distinct_ids,
    grouping(case when running_sum_quantity >= 1010 then 'LARGE' else 'SMALL' end) as grouping_level
from with_dims
group by grouping sets ((case when running_sum_quantity >= 1010 then 'LARGE' else 'SMALL' end, store_id, date_trunc('month', launch_date)), (case when running_sum_quantity >= 1010 then 'LARGE' else 'SMALL' end, store_id), (case when running_sum_quantity >= 1010 then 'LARGE' else 'SMALL' end), ())
having quantity_bit_and < 1000;
