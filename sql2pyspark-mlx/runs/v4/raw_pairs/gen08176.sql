with unpacked as (
    select
        id,
        created_at,
        coupon_codes,
        size(filter(items, item -> item.qty >= 3)) as bulk_lines,
        size(attributes) as num_attributes,
        array_join(array_sort(coupon_codes), ',') as coupons_text,
        ship_to.city as ship_city
    from carts
)
select
    *,
    max(num_attributes) over (partition by ship_city) as ship_city_max_num_attributes,
    avg(num_attributes) over (order by num_attributes, id rows between 6 preceding and current row) as num_attributes_avg_6,
    avg(bulk_lines) over (order by bulk_lines, id rows between 2 preceding and 1 preceding) as bulk_lines_avg_2_prior,
    row_number() over (partition by ship_city order by num_attributes, id) as row_seq,
    row_number() over (partition by ship_city order by bulk_lines, id) as seq_no
from unpacked
