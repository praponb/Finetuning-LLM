select a.method, b.plan from (select distinct method from payments) a cross join (select distinct plan from subscriptions) b order by method, plan
