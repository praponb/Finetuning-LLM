SELECT id, browser FROM sessions INTERSECT ALL SELECT id, browser FROM sessions WHERE device <> 'desktop'
