select a.carrier, b.plan from (select distinct carrier from shipments) a cross join (select distinct plan from subscriptions) b
