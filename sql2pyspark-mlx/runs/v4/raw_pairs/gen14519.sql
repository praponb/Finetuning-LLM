SELECT id FROM departments EXCEPT DISTINCT SELECT id FROM departments WHERE location <> 'Berlin'
