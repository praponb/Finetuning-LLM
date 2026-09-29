select hire_date, 'employee' as source from employees union select delivered_date, 'shipment' as source from shipments;
