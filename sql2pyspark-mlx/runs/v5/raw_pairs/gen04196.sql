with joined as (
    -- Attach orders attributes
    select
        e.*,
        b.discount,
        b.status as order_status,
        b.order_date
    from payments e
    join orders b
        on e.order_id = b.id
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        method,
        amount,
        status,
        discount,
        order_status,
        order_date
    from (
        select *, rank() over (partition by method order by paid_at desc) as row_num
        from joined
    ) ranked
    where row_num = 1
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        method,
        discount,
        order_status,
        order_date,
        date_format(order_date, 'yyyy-MM') as order_date_label,
        coalesce(method, 'N/A') as method_filled
    from deduped
    where amount between 610 and 720
),
metrics as (
    -- Ranking and gap features
    select
        *,
        avg(discount) over (partition by method order by discount, id rows between 2 preceding and current row) as last3_avg_discount,
        max(discount) over (partition by method order by discount, id rows between 2 preceding and current row) as last3_max_discount,
        sum(discount) over (partition by method order by discount, id rows between 2 preceding and current row) as last3_sum_discount
    from filtered
    where method = 'card'
),
totals as (
    -- Multi-level aggregation
    select
        date_trunc('year', order_date) as year_start,
        sum(last3_avg_discount) as total_last3_avg_discount,
        sum(case when order_status = 'SHIPPED' then last3_sum_discount else 0 end) as shipped_last3_sum_discount,
        sort_array(collect_set(order_status)) as order_status_list,
        size(collect_set(method)) as num_methods,
        count(*) as event_count
    from metrics
    group by date_trunc('year', order_date)
    having total_last3_avg_discount > 0
)
select
    year_start,
    total_last3_avg_discount,
    shipped_last3_sum_discount,
    num_methods,
    event_count
from totals
order by shipped_last3_sum_discount desc;
