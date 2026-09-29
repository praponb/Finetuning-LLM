select x.method, y.carrier from (select distinct method from payments) x cross join (select distinct carrier from shipments) y;
