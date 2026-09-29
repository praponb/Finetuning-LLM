with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        datediff(delivered_date, shipped_date) days_shipped_date_to_delivered_date,
        substring(status, 1, 2) status_prefix
    from shipments
    where status in ('RETURNED', 'IN_TRANSIT')
)
-- Window metrics over several different partitions
select
    *,
    max(cost) over (partition by carrier order by delivered_date, id rows between 6 preceding and current row) last7_max_cost,
    avg(cost) over (partition by carrier order by delivered_date, id rows between 6 preceding and current row) last7_avg_cost,
    min(cost) over (partition by carrier order by delivered_date, id rows between 6 preceding and current row) last7_min_cost,
    count(cost) over (partition by carrier order by delivered_date, id rows between 6 preceding and current row) last7_count_cost,
    round(cost / sum(cost) over (partition by carrier), 4) cost_share
from valid;
