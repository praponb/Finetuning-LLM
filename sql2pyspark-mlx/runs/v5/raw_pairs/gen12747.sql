with flattened as (
    -- Unnest the array into one row per element
    select
        base.id,
        base.customer_id,
        pos,
        line.price,
        line.promo
    from carts base
    lateral view outer posexplode(base.items) e as pos, line
),
enriched as (
    -- Attach customers attributes
    select
        e.customer_id,
        e.pos,
        e.promo,
        p.signup_date,
        p.name
    from flattened e
    join customers p
        on e.customer_id = p.id
),
with_dims as (
    -- Attach customers attributes
    select src.*, p.age
    from enriched src
    left join customers p
        on src.customer_id = p.id
),
kept_keys as (
    -- Set difference on keys
    select customer_id
    from with_dims
    except all
    select customer_id
    from carts
    where to_date(created_at) >= '2023-05-21'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_dims s
    join kept_keys k on s.customer_id = k.customer_id
),
matched_keys as (
    -- Set difference on keys
    select customer_id
    from kept_rows
    except all
    select customer_id
    from carts
    where customer_id = 25
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from kept_rows s
    join matched_keys k on s.customer_id = k.customer_id
)
-- Aggregate with conditional metrics
select
    customer_id,
    promo,
    count(*) as cnt,
    sort_array(collect_set(promo)) as promo_list,
    round(avg(age), 2) as avg_age,
    avg(pos) as avg_pos,
    count(*) as event_count
from reconciled
group by 1, 2
having avg(pos) >= 0;
