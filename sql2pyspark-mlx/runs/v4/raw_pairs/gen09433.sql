with valid as (
    -- Basic cleansing and defaults
    select *, initcap(trim(brand)) as brand_title
    from products
    where launch_date >= to_date('2023-05-29')
      and brand is not null
      and category in ('Grocery', 'Electronics')
),
base as (
    -- Keep recent, valid rows
    select
        *,
        array_join(tags, '|') as tags_text,
        coalesce(brand, 'DIRECT') as brand_clean
    from valid
    where launch_date > to_date('2023-06-30')
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        product_name,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        brand_title,
        brand_clean,
        substring(product_name, 1, 2) as product_name_prefix
    from base
    where launch_date > date '2023-07-08'
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by brand order by price nulls last, id) as rank_in_group
    from cleaned
    qualify rank_in_group = 1
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from ranked s
    where not exists (
            select 1
            from order_items c
            where c.product_id = s.id
              and c.unit_price is not null
        )
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by brand order by rank_in_group desc, id) as seq_no,
        last_value(price, true) over (order by rank_in_group, id rows between unbounded preceding and current row) as last_known_price
    from not_flagged
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        product_name,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        brand_title,
        brand_clean,
        product_name_prefix,
        rank_in_group,
        seq_no,
        last_known_price,
        avg(seq_no) over (order by price, id rows between 2 preceding and 1 preceding) as seq_no_avg_2_prior,
        last_value(price) ignore nulls over (partition by brand order by cost, id rows between unbounded preceding and current row) as last_known_price_2,
        percent_rank() over (order by cost nulls first) as cost_percent_rank,
        sum(cost) over (partition by brand order by rank_in_group, id rows between unbounded preceding and current row) as running_cost
    from enriched
)
-- Multi-level aggregation
select
    case when cost_percent_rank >= 0.68 then 'PREMIUM' else 'STANDARD' end as cost_percent_rank_tier,
    seq_no,
    min(stock_qty) as min_stock_qty,
    approx_percentile(seq_no, array(0.25, 0.5, 0.75)) as seq_no_quartiles,
    sum(case when brand = 'Umbrella' then seq_no else 0 end) as umbrella_seq_no
from with_windows
group by case when cost_percent_rank >= 0.68 then 'PREMIUM' else 'STANDARD' end, seq_no
having min(stock_qty) > 0
order by umbrella_seq_no desc nulls first;
