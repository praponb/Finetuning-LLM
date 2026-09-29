with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        amount,
        discount,
        length(status) status_length
    from orders
    where order_date >= date '2023-05-21'
      and amount > 870
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        order_date,
        channel,
        discount,
        status_length,
        max(amount) over (partition by customer_id) customer_id_max_amount,
        corr(amount, discount) over (partition by store_id, channel) corr_amount_discount
    from cleaned
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        count(discount) over (partition by customer_id, channel order by status_length, id rows between unbounded preceding and current row) running_count_discount,
        avg(discount) over (partition by customer_id, channel order by status_length, id rows between unbounded preceding and current row) running_avg_discount,
        min(discount) over (partition by customer_id, channel order by status_length, id rows between unbounded preceding and current row) running_min_discount,
        row_number() over (partition by channel order by order_date, id) seq_no
    from metrics
),
attributed as (
    -- Non-equi join on a validity window
    select
        e.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from enriched e
    left join promotions pr
        on e.channel = pr.channel
       and e.order_date >= pr.starts_on
       and e.order_date < pr.ends_on + interval 7 days
)
-- Native SQL UNPIVOT (wide to long)
select
    id,
    promo_id,
    promo_code,
    metric,
    val
from (
    select
        id,
        promo_id,
        promo_code,
        discount_pct,
        running_avg_discount,
        discount
    from attributed
) src
unpivot (
    val for metric in (discount_pct, running_avg_discount, discount)
);
