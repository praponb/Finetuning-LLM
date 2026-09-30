select a.channel, b.status from (select distinct channel from orders) a cross join (select distinct status from subscriptions) b;
