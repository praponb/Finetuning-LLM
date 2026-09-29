with cleaned as (
    select
        id,
        store_id,
        product_id,
        revenue,
        add_months(sale_date, 12) as sale_date_plus_months,
        last_day(sale_date) as sale_date_month_end
    from store_sales
    where sale_date >= date '2023-03-10'
)
select
    id,
    store_id,
    sale_date_plus_months,
    sale_date_month_end,
    lead(revenue) over (partition by store_id order by sale_date_month_end, id) as next_revenue,
    row_number() over (partition by store_id order by sale_date_month_end, id) as row_seq,
    avg(revenue) over (order by revenue, id rows between 6 preceding and current row) as revenue_avg_6
from cleaned
