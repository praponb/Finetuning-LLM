select carrier, sort_array(collect_set(carrier)) carrier_list from shipments where year(shipped_date) = 2023 group by carrier;
