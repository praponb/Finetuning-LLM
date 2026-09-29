with metrics as (
    -- Ranking and gap features
    select
        *,
        sum(discount) over w_grp as cumulative_discount,
        rank() over (partition by status order by amount desc nulls last) as amount_rank,
        ntile(3) over (partition by channel order by discount desc, id asc) as discount_bucket,
        ntile(4) over w_recent as discount_bucket_2
    from orders
    window w_grp as (order by order_date asc, id asc rows between unbounded preceding and current row), w_recent as (partition by store_id order by discount asc, id asc)
)
-- Aggregate with conditional metrics
select
    date_trunc('year', order_date) as year_start,
    round(sum(discount_bucket_2) / nullif(count(case when status = 'SHIPPED' then 1 end), 0), 2) as discount_bucket_2_per_shipped,
    count(distinct status) as distinct_status,
    sum(case when status = 'PENDING' then discount else 0 end) as pending_discount
from metrics
group by cube (date_trunc('year', order_date))
order by discount_bucket_2_per_shipped desc;
