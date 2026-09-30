select x.method, y.status from (select distinct method from payments) x cross join (select distinct status from shipments) y;
