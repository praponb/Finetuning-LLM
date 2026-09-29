SELECT id, browser FROM sessions EXCEPT ALL SELECT id, browser FROM sessions WHERE country = 'DE';
