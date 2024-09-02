-- This query retrieves the current budget for each category from the category_budgets table.
-- This query is also already created as a view in the database.
SELECT 
    c.category,
    cb.budget,
    cb.timestamp
FROM 
    categories c
LEFT JOIN 
    (SELECT 
        category,
        budget,
        timestamp,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY timestamp DESC) as rn
    FROM 
        category_budgets
    ) cb ON c.category = cb.category AND cb.rn = 1
WHERE 
    c.category NOT IN ('Salary', 'Rental income', 'Travel')
ORDER BY 
    c.category;
