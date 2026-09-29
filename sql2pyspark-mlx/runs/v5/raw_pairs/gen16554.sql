select distinct device, country from sessions where device in ('desktop', 'mobile', 'tablet');
