select
    case when amount >= 1380 then 'PREMIUM' else 'STANDARD' end as amount_tier,
    count_if(method = 'card') as card_events,
    sort_array(collect_set(status)) as status_list,
    count(distinct id) as distinct_ids,
    round(sum(amount) / nullif(count(case when status = 'SUCCESS' then 1 end), 0), 2) as amount_per_success,
    stddev_samp(amount) as amount_stddev
from payments
group by case when amount >= 1380 then 'PREMIUM' else 'STANDARD' end
having distinct_ids > 1
order by amount_stddev desc;
