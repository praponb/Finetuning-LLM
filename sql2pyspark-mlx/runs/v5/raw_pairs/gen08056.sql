with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        coupon_codes,
        pos,
        elem.qty,
        elem.price,
        elem.promo
    from carts
    lateral view outer posexplode(items) x as pos, elem
),
remaining_keys as (
    -- Set difference on keys
    select customer_id, id
    from exploded
    except all
    select customer_id, id
    from carts
    where to_date(created_at) >= '2024-06-01'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from exploded s
    inner join remaining_keys k on s.customer_id = k.customer_id and s.id = k.id
),
with_dims as (
    -- Attach customers attributes
    select
        e.customer_id,
        e.pos,
        b.segment,
        b.age
    from reconciled e
    inner join customers b
        on e.customer_id = b.id
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over (partition by segment order by pos nulls last) as pos_dense_rank,
        avg(pos) over (partition by segment, customer_id) as segment_avg_pos,
        count(pos) over (partition by customer_id) as customer_id_count_pos
    from with_dims
    where pos between 1 and 3
)
-- Aggregate with conditional metrics
select
    case when pos >= 3 then 'PREMIUM' else 'STANDARD' end as pos_tier,
    sort_array(collect_set(segment)) as segment_list,
    count(case when segment = 'Corporate' then 1 end) as corporate_count,
    count(case when segment = 'Corporate' then 1 end) as corporate_count_2,
    count(distinct customer_id) as distinct_customer_ids
from with_windows
group by pos_tier
