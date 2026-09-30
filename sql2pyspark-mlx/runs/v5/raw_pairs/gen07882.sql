with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        ceil(revenue) revenue_whole,
        date_format(sale_date, 'MMM yyyy') sale_date_label
    from store_sales
    where sale_date >= date '2023-03-22'
      and sale_date <= '2024-10-01'
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by product_id, store_id order by sale_date desc nulls last, id) as rank_in_group
        from cleaned
    ) x
    where rank_in_group = 1
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        product_id,
        sale_date,
        revenue,
        revenue_whole,
        rank_in_group,
        lead(revenue_whole) over w_running next_revenue_whole,
        lag(revenue_whole) over w_running prev_revenue_whole,
        lag(units) over w_running prev_units,
        row_number() over w_running row_seq,
        corr(revenue, units) over (partition by product_id, store_id) corr_revenue_units
    from ranked
    window w_running as (partition by store_id, product_id order by revenue, id)
),
with_windows_store_sales as (
    -- Ranking and gap features
    select
        id,
        sale_date,
        revenue,
        revenue_whole,
        rank_in_group,
        prev_units,
        row_seq,
        row_number() over (partition by product_id order by rank_in_group desc, id) event_seq,
        last_value(row_seq, true) over (partition by product_id order by prev_revenue_whole, id rows between unbounded preceding and current row) last_known_row_seq,
        row_number() over w_part row_seq_2
    from with_windows
    where sale_date between '2023-03-01' and '2024-04-20'
    window w_part as (partition by product_id order by row_seq desc, id)
)
select
    id,
    sale_date,
    revenue,
    prev_units,
    row_seq,
    event_seq,
    last_known_row_seq
from with_windows_store_sales
order by revenue desc;
