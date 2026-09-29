select cost from shipments where carrier != 'Flash' union select shipping_fee from orders;
