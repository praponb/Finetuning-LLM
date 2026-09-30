with cleaned as (
    select *, quantity * unit_price as line_total
    from order_items
    where unit_price <= 330
),
metrics as (
    select
        id,
        order_id,
        product_id,
        quantity,
        avg(unit_price) over (partition by order_id, product_id order by unit_price, id rows between 6 preceding and current row) as unit_price_avg_6,
        sum(unit_price) over (partition by product_id order by quantity, id rows between unbounded preceding and current row) as running_unit_price
    from cleaned
    where line_total > 55
),
with_windows as (
    select
        id,
        unit_price_avg_6,
        running_unit_price,
        row_number() over (order by running_unit_price, id) as row_seq,
        round(running_unit_price / sum(running_unit_price) over (partition by product_id), 4) as running_unit_price_share,
        row_number() over w_recent as row_seq_2
    from metrics
    window w_recent as (partition by product_id order by unit_price_avg_6, id)
),
enriched as (
    select
        id,
        unit_price_avg_6,
        running_unit_price,
        row_seq,
        max(row_seq) over w_hist as last7_max_row_seq,
        min(row_seq) over w_hist as last7_min_row_seq,
        sum(row_seq) over w_hist as last7_sum_row_seq
    from with_windows
    window w_hist as (order by unit_price_avg_6, id rows between 6 preceding and current row)
)
select *
from (
    select
        id,
        last7_sum_row_seq,
        last7_max_row_seq,
        last7_min_row_seq
    from enriched
) src
unpivot (
    metric_value for kpi in (last7_sum_row_seq as LAST7_SUM_ROW_SEQ_VAL, last7_max_row_seq, last7_min_row_seq as LAST7_MIN_ROW_SEQ)
);
