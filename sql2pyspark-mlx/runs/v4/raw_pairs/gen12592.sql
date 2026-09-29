with deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by product_id order by units desc nulls last, id) as latest_rank
        from store_sales
    ) ranked
    where latest_rank = 1
),
enriched as (
    -- Attach products attributes
    select
        e.*,
        b.product_name,
        b.cost,
        b.category
    from deduped e
    left join products b
        on e.product_id = b.id
),
valid as (
    -- Keep recent, valid rows
    select *, floor(units) as units_whole
    from enriched
    where latest_rank >= 1
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        last_value(cost, true) over (partition by store_id order by latest_rank, id rows between unbounded preceding and current row) as last_known_cost,
        row_number() over (partition by product_id, category order by units, id) as seq_no
    from valid
),
deduped_store_sales as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, rank() over (partition by category order by units_whole nulls last) as latest_rank_2
        from with_windows
    ) t
    where latest_rank_2 = 1
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_id,
        latest_rank,
        product_name,
        cost,
        category,
        units_whole,
        last_known_cost,
        seq_no,
        latest_rank_2,
        last_day(sale_date) as sale_date_month_end,
        latest_rank_2 * units_whole as latest_rank_2_times_units_whole,
        coalesce(category, 'N/A') as category_clean
    from deduped_store_sales
    where sale_date >= '2024-08-06'
),
valid_store_sales as (
    -- Keep recent, valid rows
    select
        *,
        concat_ws('-', product_name, category) as product_name_category_key,
        coalesce(category, 'N/A') as category_filled
    from filtered
    where latest_rank_2 < 1
)
select
    id,
    product_id,
    latest_rank,
    product_name,
    units_whole,
    last_known_cost,
    product_name_category_key
from valid_store_sales
