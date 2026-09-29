with latest as (
    -- QUALIFY-style window filter
    select *
    from order_items
    qualify row_number() over (partition by product_id order by quantity, id) = 1
),
with_dims as (
    -- Attach orders attributes
    select
        src.*,
        d.channel,
        d.shipping_fee
    from latest src
    left join orders d
        on src.order_id = d.id
),
enriched as (
    -- Attach orders attributes
    select
        e.*,
        p.amount,
        p.channel as order_channel,
        p.discount
    from with_dims e
    left join orders p
        on e.order_id = p.id
),
joined as (
    -- Attach orders attributes
    select s.*, b.shipping_fee as order_shipping_fee
    from enriched s
    left join orders b
        on s.order_id = b.id
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(channel) ignore nulls over (partition by product_id order by amount, id rows between unbounded preceding and current row) as first_known_channel,
        avg(unit_price) over (partition by order_id) as order_id_avg_unit_price,
        corr(quantity, unit_price) over (partition by order_id) as corr_quantity_unit_price,
        avg(shipping_fee) over (order by quantity, id rows between 2 preceding and 1 preceding) as shipping_fee_avg_2_prior
    from joined
)
-- Grouped metrics
select
    case when corr_quantity_unit_price >= 0.57 then 'LARGE' else 'SMALL' end as corr_quantity_unit_price_tier,
    max(amount) filter (where channel = 'partner') as partner_max_amount,
    percentile_approx(quantity, array(0.25, 0.5, 0.75)) as quantity_quartiles,
    count(*) filter (where channel = 'mobile') as mobile_rows,
    sort_array(collect_set(channel)) as channel_list,
    count(case when order_channel = 'mobile' then 1 end) as mobile_count,
    sum(shipping_fee_avg_2_prior) filter (where order_channel = 'web') as web_total_shipping_fee_avg_2_prior
from scored
group by 1
having max(amount) filter (where channel = 'partner') < 0
order by web_total_shipping_fee_avg_2_prior desc nulls last;
