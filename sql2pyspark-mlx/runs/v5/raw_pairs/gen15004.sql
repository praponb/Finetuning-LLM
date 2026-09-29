with filtered as (
    -- Keep recent, valid rows
    select
        id,
        supplier_name,
        city,
        trim(supplier_name) clean_supplier_name
    from suppliers
    where city is not null
      and rating between 2.5 and 3.3
),
latest_match as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select s.*, top1.price
    from filtered s
    left join lateral (
            select c.price
            from products c
            where c.supplier_id = s.id
            order by c.cost desc, c.id desc
            limit 1
        ) top1 on true
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by city order by price desc, id) as rank_in_group
    from latest_match
    qualify rank_in_group = 1
),
latest_match_suppliers as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.product_price,
        top1.category
    from ranked s
    left join lateral (
            select c.price as product_price, c.category
            from products c
            where c.supplier_id = s.id
            order by c.stock_qty desc, c.id desc
            limit 1
        ) top1 on true
),
filtered_suppliers as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when category = 'Fashion' then 1 else 0 end is_fashion,
        round(price * 1.1, 2) price_adjusted
    from latest_match_suppliers
    where city is not null
      and category is distinct from 'Sports'
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        supplier_name,
        price,
        product_price,
        is_fashion,
        price_adjusted,
        max(price) over w_hist running_max_price,
        count(price) over w_hist running_count_price,
        sum(price) over w_hist running_sum_price,
        percent_rank() over (partition by city order by price_adjusted nulls first) price_adjusted_percent_rank
    from filtered_suppliers
    where price_adjusted is null
    window w_hist as (partition by category order by is_fashion, id rows between unbounded preceding and current row)
),
stats as (
    -- Grouped metrics
    select
        case when running_count_price >= 15 then 'LARGE' else 'SMALL' end as running_count_price_tier,
        case when price_adjusted_percent_rank >= 0.69 then 'HIGH' else 'LOW' end as price_adjusted_percent_rank_tier,
        approx_percentile(running_max_price, 0.5) as running_max_price_p50,
        round(avg(price_adjusted_percent_rank), 2) as avg_price_adjusted_percent_rank,
        count(*) as num_rows
    from enriched
    group by grouping sets ((case when running_count_price >= 15 then 'LARGE' else 'SMALL' end, case when price_adjusted_percent_rank >= 0.69 then 'HIGH' else 'LOW' end), (case when running_count_price >= 15 then 'LARGE' else 'SMALL' end))
    having round(avg(price_adjusted_percent_rank), 2) > 0
)
select
    running_count_price_tier,
    price_adjusted_percent_rank_tier,
    num_rows
from stats;
