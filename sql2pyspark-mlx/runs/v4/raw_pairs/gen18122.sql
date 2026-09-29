select page, id from web_events intersect all select page, id from web_events where duration_sec between 168 and 279
