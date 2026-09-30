select x.status, y.carrier from (select distinct status from payments) x cross join (select distinct carrier from shipments) y order by status, carrier;
