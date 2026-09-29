with valid as (
    -- Filter the raw rows and derive helper columns
    select *, unit_price - quantity as line_total
    from order_items
    where product_id is not null
),
enriched as (
    -- Attach orders attributes
    select src.*, o.discount
    from valid src
    join orders o
        on src.order_id = o.id
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        sum(unit_price) over (partition by order_id) as order_id_sum_unit_price,
        first_value(line_total) ignore nulls over (order by discount, id rows between unbounded preceding and current row) as first_known_line_total
    from enriched
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from with_windows
    qualify row_number() over (partition by order_id, product_id order by first_known_line_total nulls first, id) = 1
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by order_id, product_id order by unit_price, id) as dedup_rank
        from ranked
    ) ranked
    where dedup_rank = 1
),
enriched_order_items as (
    -- Attach products attributes
    select a.*, p.product_name
    from deduped a
    join products p
        on a.product_id = p.id
),
joined as (
    -- Attach products attributes
    select
        s.*,
        d.category,
        d.brand,
        d.cost
    from enriched_order_items s
    left join products d
        on s.product_id = d.id
),
summary as (
    -- Multi-level aggregation
    select
        product_id,
        case when unit_price >= 320 then 'LARGE' else 'SMALL' end as unit_price_tier,
        round(sum(order_id_sum_unit_price) / nullif(count(case when brand = 'Initech' then 1 end), 0), 2) as order_id_sum_unit_price_per_initech,
        size(collect_set(category)) as num_categories,
        count_if(category = 'Home') as home_events,
        sum(case when brand = 'Stark' then order_id_sum_unit_price else 0 end) as stark_order_id_sum_unit_price,
        sort_array(collect_set(category)) as category_list
    from joined
    group by grouping sets ((product_id, case when unit_price >= 320 then 'LARGE' else 'SMALL' end), (product_id))
)
select
    unit_price_tier,
    order_id_sum_unit_price_per_initech,
    num_categories,
    home_events,
    category_list
from summary
order by num_categories nulls last;
