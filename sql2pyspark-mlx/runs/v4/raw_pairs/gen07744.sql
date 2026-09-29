with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    from carts
    lateral view outer explode(items) x as elem
),
enriched as (
    -- Ranking and gap features
    select
        *,
        round(qty / sum(qty) over (partition by promo), 4) as qty_share,
        dense_rank() over w_grp as price_dense_rank
    from flattened
    window w_grp as (partition by promo order by price asc nulls first)
),
kept_keys as (
    -- Set difference on keys
    select id
    from enriched
    except
    select id
    from carts
    where customer_id in (10, 30, 39)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join kept_keys k on s.id = k.id
),
base as (
    -- Keep recent, valid rows
    select
        *,
        case when promo = 'WELCOME5' then 1 else 0 end as is_welcome5,
        coalesce(promo, 'UNKNOWN') as promo_filled
    from kept_rows
    where promo is not null
),
totals as (
    -- Aggregate with conditional metrics
    select
        case when price_dense_rank >= 7 then 'HIGH' else 'LOW' end as price_dense_rank_tier,
        count(distinct promo) as distinct_promos,
        sum(case when promo = 'SPRING10' then price_dense_rank else 0 end) as spring10_price_dense_rank,
        count(*) as row_count
    from base
    group by case when price_dense_rank >= 7 then 'HIGH' else 'LOW' end
)
select
    price_dense_rank_tier,
    distinct_promos,
    spring10_price_dense_rank,
    row_count
from totals
order by row_count desc, spring10_price_dense_rank asc;
