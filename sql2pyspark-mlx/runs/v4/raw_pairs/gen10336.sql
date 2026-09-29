with eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from products s
    where not exists (
            select 1
            from inventory ch
            where ch.product_id = s.id
              and ch.product_id in (2, 3)
        )
),
joined as (
    -- Attach suppliers attributes
    select
        a.*,
        b.rating,
        b.supplier_name
    from eligible a
    inner join suppliers b
        on a.supplier_id = b.id
),
joined_products as (
    -- Attach suppliers attributes
    select
        a.*,
        b.country,
        b.city
    from joined a
    left join suppliers b
        on a.supplier_id = b.id
)
-- Aggregate with conditional metrics
select
    supplier_id,
    date_trunc('year', launch_date) as year_start,
    category,
    approx_percentile(cost, 0.9) as cost_p90,
    count(case when country = 'DE' then 1 end) as de_count,
    avg(rating) as avg_rating
from joined_products
group by cube (supplier_id, date_trunc('year', launch_date), category)
order by avg_rating desc nulls last
