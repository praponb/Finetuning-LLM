with top_subscriptions as (select *, rank() over (partition by plan order by start_date desc) as rnk from subscriptions) select * from top_subscriptions where rnk <= 3;
