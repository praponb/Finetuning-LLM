select id, shipped_date, delivered_date from shipments where shipped_date >= (select max(shipped_date) from shipments where delivered_date < '2025-09-01') order by shipped_date, delivered_date;
