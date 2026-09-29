SELECT * FROM (SELECT firmware, device_code, status_bits FROM device_readings) src PIVOT (AVG(status_bits) FOR device_code IN ('DV-03' AS dv_03, 'DV-02' AS dv_02, 'DV-04' AS dv_04));
