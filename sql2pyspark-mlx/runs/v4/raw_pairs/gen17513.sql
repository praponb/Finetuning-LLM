SELECT id, browser FROM sessions WHERE browser IN ('Edge', 'Firefox', 'Chrome') AND device IN ('tablet', 'desktop', 'mobile') AND browser <> 'Firefox'
