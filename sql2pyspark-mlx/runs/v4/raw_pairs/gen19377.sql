with valid as (
    select *, add_months(sale_date, 3) sale_date_plus_months
    from store_sales
    where revenue between 1600 and 1800
),
joined as (
    select
        src.*,
        b.brand,
        b.category,
        b.price
    from valid src
    left join products b
        on src.product_id = b.id
),
with_windows as (
    select
        *,
        sum(units) over (partition by brand) brand_sum_units,
        lead(price, 1) over (partition by store_id, product_id order by revenue, id) next_price,
        first_value(category) ignore nulls over (partition by category order by sale_date, id rows between unbounded preceding and current row) first_known_category
    from joined
),
enriched as (
    select
        *,
        avg(price) over w_part price_avg_6,
        corr(units, revenue) over w_running corr_units_revenue
    from with_windows
    window w_part as (partition by category order by sale_date_plus_months, id rows between 6 preceding and current row), w_running as (partition by brand)
),
latest as (
    select *
    from (
        select *, row_number() over (partition by category order by next_price desc nulls first, id) as latest_rank
        from enriched
    ) t
    where latest_rank = 1
),
deduped as (
    select *
    from latest
    qualify row_number() over (partition by brand order by brand_sum_units, id) = 1
)
select
    date_trunc('week', sale_date_plus_months) as week_start,
    count(case when category = 'Fashion' then 1 end) as fashion_count,
    percentile_approx(next_price, array(0.25, 0.5, 0.75)) as next_price_quartiles,
    percentile_approx(units, 0.5) as units_p50,
    round(avg(price), 2) as avg_price,
    bit_and(units) as units_bit_and
from deduped
group by 1
order by fashion_count desc nulls last
