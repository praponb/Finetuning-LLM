select status, sort_array(collect_set(carrier)) as carrier_list from shipments group by status order by status;
