select
    *,
    row_number() over w_hist as seq_no,
    ntile(4) over w_part as discount_pct_bucket,
    stddev_samp(discount_pct) over w_recent as promo_code_stddev_samp_discount_pct,
    avg(discount_pct) over (partition by channel order by ends_on, id rows between 3 preceding and current row) as discount_pct_avg_3,
    avg(discount_pct) over w_grp as discount_pct_avg_2
from promotions
window w_hist as (order by ends_on, id), w_part as (order by discount_pct desc, id), w_recent as (partition by promo_code, channel), w_grp as (order by starts_on, id rows between 2 preceding and current row);
