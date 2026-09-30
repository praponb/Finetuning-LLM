with not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from products s
    where not exists (
            select 1
            from inventory c
            where c.product_id = s.id
              and c.last_restock < s.launch_date
        )
),
enriched as (
    -- Attach suppliers attributes
    select
        s.id,
        s.product_name,
        s.category,
        s.brand,
        s.price,
        s.stock_qty,
        s.tags,
        s.supplier_id,
        b.supplier_name,
        b.rating
    from not_flagged s
    left join suppliers b
        on s.supplier_id = b.id
    where b.supplier_name like '%Siam%'
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        array_join(tags, '|') as tags_text,
        initcap(trim(brand)) as brand_title
    from enriched
    where rating >= 3.8
),
scored as (
    -- Ranking and gap features
    select
        id,
        product_name,
        price,
        tags,
        supplier_id,
        rating,
        brand_title,
        row_number() over (order by rating, id) as row_seq,
        count(rating) over (partition by brand) as brand_count_rating,
        avg(price) over (order by price, id rows between 2 preceding and current row) as price_avg_2
    from valid
),
scored_products as (
    -- Ranking and gap features
    select
        *,
        cume_dist() over (order by price_avg_2 nulls last) as price_avg_2_cume_dist,
        first_value(price_avg_2) ignore nulls over (order by row_seq, id rows between unbounded preceding and current row) as first_known_price_avg_2
    from scored
)
-- Multi-level aggregation
select
    supplier_id,
    case when row_seq >= 7 then 'LARGE' else 'SMALL' end as row_seq_tier,
    sum(price_avg_2) as total_price_avg_2,
    percentile_approx(brand_count_rating, 0.9) as brand_count_rating_p90,
    approx_percentile(first_known_price_avg_2, 0.95) as first_known_price_avg_2_p95,
    grouping_id() as grouping_id
from scored_products
group by cube (supplier_id, case when row_seq >= 7 then 'LARGE' else 'SMALL' end)
order by supplier_id desc nulls first
