with joined as (
    -- Attach products attributes
    select
        e.*,
        p.cost,
        p.price,
        p.brand
    from inventory e
    left join products p
        on e.product_id = p.id
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        cost,
        price,
        brand,
        row_number() over (partition by product_id order by cost desc, id) as seq_no,
        sum(price) over (partition by store_id order by quantity, id rows between unbounded preceding and current row) as running_price
    from joined
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from metrics
    qualify row_number() over (partition by product_id, brand order by last_restock nulls last, id) = 1
)
-- Multi-level aggregation
select
    case when seq_no >= 7 then 'LARGE' else 'SMALL' end as seq_no_tier,
    count(*) as event_count,
    sum(case when brand = 'Globex' then running_price else 0 end) as globex_running_price
from deduped
group by case when seq_no >= 7 then 'LARGE' else 'SMALL' end
order by globex_running_price nulls last
