with valid as (
    -- Keep recent, valid rows
    select
        id,
        order_date,
        status,
        channel,
        discount,
        shipping_fee,
        cast(amount as int) as amount_int,
        coalesce(amount, 0) as amount_filled
    from orders
    where order_date > date '2023-05-08'
),
with_window as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code,
        pr.discount_pct
    from valid s
    left join promotions pr
        on coalesce(s.channel, 'web') = pr.channel
       and s.order_date >= pr.starts_on
       and s.order_date < pr.ends_on + interval '30' day
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        min(amount_int) over (partition by status, promo_code order by promo_id, id, promo_id rows between 6 preceding and current row) as last7_min_amount_int,
        sum(amount_int) over (partition by status, promo_code order by promo_id, id, promo_id rows between 6 preceding and current row) as last7_sum_amount_int,
        max(amount_int) over (partition by status, promo_code order by promo_id, id, promo_id rows between 6 preceding and current row) as last7_max_amount_int
    from with_window
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by channel order by order_date desc) as dedup_rank
    from metrics
    qualify dedup_rank = 1
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from ranked s
    where exists (
            select 1
            from payments ch
            where ch.order_id = s.id
              and ch.paid_at < s.order_date
        )
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        order_date,
        status,
        discount,
        amount_filled,
        promo_id,
        promo_code,
        last7_min_amount_int,
        percent_rank() over (order by amount_int) as amount_int_percent_rank,
        lead(discount, 1, 0) over (partition by promo_code order by amount_filled, id, promo_id) as next_discount,
        lag(discount, 2) over (partition by channel order by order_date, id, promo_id) as prev_discount
    from kept
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        promo_id,
        status,
        next_discount,
        amount_filled
    from with_windows
) src
unpivot (
    val for metric in (next_discount as NEXT_DISCOUNT, amount_filled as AMOUNT_FILLED_VAL)
) u
