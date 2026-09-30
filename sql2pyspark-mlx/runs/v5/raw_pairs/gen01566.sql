with valid as (
    select
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        size(tags) as tags_count
    from products
    where launch_date > to_date('2023-08-13')
      and length(product_name) < 13
),
scored as (
    select
        id,
        product_name,
        stock_qty,
        launch_date,
        tags_count,
        max(tags_count) over w_part as running_max_tags_count,
        count(tags_count) over w_part as running_count_tags_count,
        min(tags_count) over w_part as running_min_tags_count,
        round(cost / sum(cost) over (partition by category), 4) as cost_share
    from valid
    window w_part as (partition by brand, category order by stock_qty, id rows between unbounded preceding and current row)
),
with_windows as (
    select
        id,
        product_name,
        stock_qty,
        launch_date,
        avg(running_count_tags_count) over (order by running_max_tags_count, id rows between 3 preceding and current row) as running_count_tags_count_avg_3,
        row_number() over (order by tags_count desc, id) as row_seq
    from scored
),
eligible as (
    select s.*
    from with_windows s
    where not exists (
            select 1
            from inventory ch
            where ch.product_id = s.id
              and ch.last_restock < s.launch_date
        )
),
filtered as (
    select *, date_sub(launch_date, 64) as launch_date_minus_64d
    from eligible
    where launch_date >= to_date('2023-04-16')
),
matched_keys as (
    select id
    from filtered
    intersect all
    select id
    from products
    where brand = 'Umbrella'
),
reconciled as (
    select s.*
    from filtered s
    join matched_keys k on s.id = k.id
),
totals as (
    select
        case when row_seq >= 8 then 'LARGE' else 'SMALL' end as row_seq_tier,
        date_trunc('year', launch_date_minus_64d) as year_start,
        case when stock_qty >= 363 then 'HIGH' else 'LOW' end as stock_qty_tier,
        approx_percentile(row_seq, 0.95) as row_seq_p95,
        bit_or(stock_qty) as stock_qty_bit_or,
        count(distinct id) as distinct_ids,
        count(distinct id) as distinct_ids_2,
        approx_percentile(stock_qty, 0.9) as stock_qty_p90
    from reconciled
    group by row_seq_tier, year_start, stock_qty_tier
)
select
    row_seq_tier,
    year_start,
    stock_qty_tier,
    row_seq_p95
from totals
order by row_seq_tier desc nulls last, row_seq_p95
