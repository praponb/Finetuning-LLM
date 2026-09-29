select x.method, y.plan from (select distinct method from payments) x cross join (select distinct plan from subscriptions) y order by method, plan
