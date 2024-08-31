WITH RECURSIVE date_range AS (
    SELECT DATE_TRUNC('month', MIN("Transaction Date")) AS month
    FROM consolidated_transactions
    
    UNION ALL
    
    SELECT DATE_TRUNC('month', month + INTERVAL '1 month')
    FROM date_range
    WHERE month < (SELECT DATE_TRUNC('month', MAX("Transaction Date")) FROM consolidated_transactions)
),
categories AS (
    SELECT DISTINCT category
    FROM consolidated_transactions
    WHERE category IS NOT NULL
),
monthly_sums AS (
    SELECT 
        dr.month,
        c.category,
        COALESCE(SUM(ct.amount), 0) AS monthly_total
    FROM date_range dr
    CROSS JOIN categories c
    LEFT JOIN consolidated_transactions ct 
        ON DATE_TRUNC('month', ct."Transaction Date") = dr.month
        AND ct.category = c.category
    GROUP BY dr.month, c.category
),
category_stats AS (
    SELECT 
        category,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ABS(monthly_total)), 2) AS p50_monthly_sum,
        ROUND(PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY ABS(monthly_total)), 2) AS p85_monthly_sum,
        ROUND(AVG(ABS(monthly_total)), 2) AS avg_monthly_sum,
        ROUND(STDDEV_POP(ABS(monthly_total)), 2) AS stddev_monthly_sum,
        COUNT(*) FILTER (WHERE monthly_total != 0) AS months_with_spending
    FROM monthly_sums
    GROUP BY category
),
latest_month_sums AS (
    SELECT 
        c.category,
        COALESCE(SUM(ct.amount), 0) AS latest_month_sum
    FROM categories c
    LEFT JOIN consolidated_transactions ct 
        ON c.category = ct.category
        AND DATE_TRUNC('month', ct."Transaction Date") = (SELECT MAX(DATE_TRUNC('month', "Transaction Date")) FROM consolidated_transactions)
    GROUP BY c.category
)
SELECT 
    cs.*,
    CASE 
        WHEN cs.avg_monthly_sum = 0 THEN NULL
        ELSE ROUND((cs.stddev_monthly_sum / cs.avg_monthly_sum) * 100, 2)
    END AS avg_percent_variance,
    ROUND(COALESCE(lms.latest_month_sum, 0), 2) AS latest_month_sum
FROM category_stats cs
LEFT JOIN latest_month_sums lms ON cs.category = lms.category
ORDER BY ROUND(COALESCE(lms.latest_month_sum, 0), 2) ASC;
