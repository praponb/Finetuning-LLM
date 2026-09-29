with base as (
    -- Keep recent, valid rows
    select
        id,
        supplier_name,
        rating,
        upper(trim(supplier_name)) as supplier_name_normalized
    from suppliers
    where city is not null
),
latest_match as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select s.*, latest.category
    from base s
    join lateral (
            select c.category
            from products c
            where c.supplier_id = s.id
            order by c.price desc, c.id desc
            limit 1
        ) latest on true
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from latest_match s
    where exists (
            select 1
            from products x
            where x.supplier_id = s.id
              and x.brand = 'Umbrella'
        )
),
with_latest as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.product_launch_date,
        top1.product_category
    from eligible s
    join lateral (
            select c.launch_date as product_launch_date, c.category as product_category
            from products c
            where c.supplier_id = s.id
            order by c.launch_date desc, c.id desc
            limit 1
        ) top1 on true
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        supplier_name_normalized,
        product_category,
        lead(rating) over w_part as next_rating,
        row_number() over w_part as seq_no
    from with_latest
    window w_part as (partition by category, product_category order by product_launch_date, id)
),
with_windows_suppliers as (
    -- Running and moving aggregates
    select
        id,
        product_category,
        seq_no,
        sum(seq_no) over (partition by product_category order by seq_no, id rows between unbounded preceding and current row) as cumulative_seq_no,
        min(seq_no) over w_recent as product_category_min_seq_no
    from with_windows
    window w_recent as (partition by product_category)
)
-- Aggregate with conditional metrics
select
    case when product_category_min_seq_no >= 7 then 'HIGH' else 'LOW' end as product_category_min_seq_no_tier,
    seq_no,
    case when seq_no >= 7 then 'LARGE' else 'SMALL' end as seq_no_tier,
    count(case when product_category = 'Home' then 1 end) as home_count,
    size(collect_set(product_category)) as num_product_categories,
    round(avg(product_category_min_seq_no), 2) as avg_product_category_min_seq_no,
    percent_rank() over (partition by case when product_category_min_seq_no >= 7 then 'HIGH' else 'LOW' end order by round(avg(product_category_min_seq_no), 2) desc) as avg_product_category_min_seq_no_percent_rank
from with_windows_suppliers
group by 1, 2, 3
