select x.status, y.plan from (select distinct status from accounts) x cross join (select distinct plan from subscriptions) y order by status, plan
