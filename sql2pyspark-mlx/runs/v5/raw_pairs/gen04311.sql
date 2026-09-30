with filtered as (
    -- Basic cleansing and defaults
    select *, date_format(delivered_date, 'yyyy-MM-dd') as delivered_date_label
    from shipments
    where delivered_date >= date '2023-09-23'
      and cost is not null
),
enriched as (
    -- Ranking and gap features
    select
        *,
        max(cost) over (partition by status order by shipped_date, id rows between 2 preceding and current row) as last3_max_cost,
        sum(cost) over (partition by status order by shipped_date, id rows between 2 preceding and current row) as last3_sum_cost,
        avg(cost) over (partition by status order by shipped_date, id rows between 2 preceding and current row) as last3_avg_cost,
        ntile(3) over (partition by carrier order by cost desc, id) as cost_bucket
    from filtered
),
enriched_shipments as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        src.carrier,
        src.cost,
        src.last3_max_cost,
        src.last3_sum_cost,
        src.last3_avg_cost,
        src.cost_bucket,
        p.discount
    from enriched src
    left join orders p
        on src.order_id = p.id
    where p.store_id = 31
),
enriched_shipments_2 as (
    -- Ranking and gap features
    select
        *,
        sum(last3_avg_cost) over (partition by order_id order by last3_sum_cost, id rows between 2 preceding and current row) as last3_sum_last3_avg_cost,
        max(last3_avg_cost) over (partition by order_id order by last3_sum_cost, id rows between 2 preceding and current row) as last3_max_last3_avg_cost,
        avg(last3_avg_cost) over (partition by order_id order by last3_sum_cost, id rows between 2 preceding and current row) as last3_avg_last3_avg_cost
    from enriched_shipments
)
-- Aggregate with conditional metrics
select
    case when last3_avg_last3_avg_cost >= 88 then 'PREMIUM' else 'STANDARD' end as last3_avg_last3_avg_cost_tier,
    size(collect_set(carrier)) as num_carriers,
    sort_array(collect_set(carrier)) as carrier_list,
    size(collect_set(carrier)) as num_carriers_2,
    round(sum(last3_sum_last3_avg_cost) / nullif(count(case when carrier = 'FedEx' then 1 end), 0), 2) as last3_sum_last3_avg_cost_per_fedex,
    percentile_approx(last3_max_last3_avg_cost, array(0.25, 0.5, 0.75)) as last3_max_last3_avg_cost_quartiles
from enriched_shipments_2
group by case when last3_avg_last3_avg_cost >= 88 then 'PREMIUM' else 'STANDARD' end
order by last3_avg_last3_avg_cost_tier desc
