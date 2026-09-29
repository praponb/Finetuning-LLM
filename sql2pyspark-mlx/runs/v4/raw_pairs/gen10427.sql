select device_code, id from device_readings intersect all select device_code, id from device_readings where humidity > 47
