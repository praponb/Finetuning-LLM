with with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        brand,
        launch_date,
        supplier_id,
        avg(cost) over (order by cost asc, id asc rows between 6 preceding and 1 preceding) as cost_avg_6_prior,
        corr(cost, price) over (partition by supplier_id, brand) as corr_cost_price
    from products
    where price > 960
)
-- Multi-level aggregation
select
    case when corr_cost_price >= 0.34 then 'HIGH' else 'LOW' end as corr_cost_price_tier,
    date_trunc('quarter', launch_date) as quarter_start,
    round(sum(cost_avg_6_prior) / nullif(count(case when brand = 'Acme' then 1 end), 0), 2) as cost_avg_6_prior_per_acme,
    count(distinct supplier_id) as distinct_supplier_ids,
    approx_percentile(corr_cost_price, array(0.25, 0.5, 0.75)) as corr_cost_price_quartiles
from with_windows
group by cube (case when corr_cost_price >= 0.34 then 'HIGH' else 'LOW' end, date_trunc('quarter', launch_date))
order by corr_cost_price_tier asc;
