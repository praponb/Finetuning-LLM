with valid as (
    -- Keep recent, valid rows
    select
        *,
        substring(city, 1, 3) as city_prefix,
        coalesce(city, 'UNKNOWN') as resolved_city
    from suppliers
    where country in ('US', 'TH', 'UK')
),
latest_match as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.price,
        top1.launch_date
    from valid s
    left join lateral (
            select c.price, c.launch_date
            from products c
            where c.supplier_id = s.id
            order by c.price desc, c.id desc
            limit 1
        ) top1 on true
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from latest_match s
    where not exists (
            select 1
            from products c
            where c.supplier_id = s.id
              and array_contains(c.tags, 'sale')
        )
),
scored as (
    -- Running and moving aggregates
    select
        id,
        city,
        resolved_city,
        avg(price) over (order by rating, id rows between 3 preceding and 1 preceding) as price_avg_3_prior,
        ntile(3) over (partition by country order by rating, id) as rating_bucket,
        sum(rating) over (partition by country order by price, id rows between unbounded preceding and current row) as running_rating
    from eligible
    where country = 'JP'
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from scored s
    where not exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and month(x.launch_date) in (3, 10, 12)
        )
)
select
    id,
    city,
    resolved_city,
    price_avg_3_prior,
    rating_bucket,
    running_rating
from kept
order by id nulls last;
