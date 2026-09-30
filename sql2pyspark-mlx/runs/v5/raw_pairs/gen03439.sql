with filtered as (
    -- Keep recent, valid rows
    select *, last_day(sale_date) as sale_date_month_end
    from store_sales
    where sale_date >= to_date('2023-09-13')
),
deduped as (
    -- QUALIFY-style window filter
    select
        id,
        sale_date,
        revenue,
        sale_date_month_end
    from (
        select *, row_number() over (partition by store_id, product_id order by revenue nulls first, id) as row_num
        from filtered
    ) ranked
    where row_num = 1
),
enriched as (
    -- Ranking and gap features
    select
        *,
        lead(revenue) over w_running as next_revenue,
        row_number() over w_running as seq_no,
        row_number() over w_running as row_seq
    from deduped
    window w_running as (order by revenue, id)
),
scored as (
    -- Ranking and gap features
    select
        *,
        sum(next_revenue) over w_grp as cumulative_next_revenue,
        sum(row_seq) over w_grp as cumulative_row_seq
    from enriched
    window w_grp as (order by next_revenue, id rows between unbounded preceding and current row)
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (order by cumulative_row_seq desc nulls last) as cumulative_row_seq_cume_dist,
        avg(cumulative_next_revenue) over (order by cumulative_row_seq, id rows between 6 preceding and 1 preceding) as cumulative_next_revenue_avg_6_prior
    from scored
)
-- Window metrics over several different partitions
select
    id,
    sale_date,
    revenue,
    sale_date_month_end,
    next_revenue,
    seq_no,
    row_seq,
    cumulative_row_seq,
    cumulative_next_revenue_avg_6_prior,
    avg(revenue) over (order by next_revenue, id rows between 3 preceding and 1 preceding) as revenue_avg_3_prior,
    row_number() over (order by next_revenue desc, id) as row_seq_2,
    sum(row_seq) over (order by cumulative_next_revenue_avg_6_prior, id rows between unbounded preceding and current row) as running_row_seq
from metrics;
