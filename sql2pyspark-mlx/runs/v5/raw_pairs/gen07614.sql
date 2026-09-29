SELECT * FROM (SELECT firmware, device_code, humidity FROM device_readings) src PIVOT (AVG(humidity) FOR device_code IN ('DV-03' AS dv_03, 'DV-04' AS dv_04, 'DV-02' AS dv_02));
