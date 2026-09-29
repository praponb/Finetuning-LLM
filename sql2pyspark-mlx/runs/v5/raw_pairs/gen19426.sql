select status, carrier from shipments intersect all select status, carrier from shipments where carrier in ('DHL', 'FedEx');
