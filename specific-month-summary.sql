WITH specified_month AS (
    SELECT MAKE_DATE(CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(1 AS BIGINT)) AS month
),
category_list AS (
    SELECT DISTINCT category, category_group
    FROM categories
    WHERE category IS NOT NULL
),
monthly_sums AS (
    SELECT 
        DATE_TRUNC('month', ct."Transaction Date") AS month,
        c.category,
        c.category_group,
        COALESCE(SUM(ct.amount), 0) AS monthly_total
    FROM category_list c
    LEFT JOIN consolidated_transactions ct ON c.category = ct.category
    GROUP BY DATE_TRUNC('month', ct."Transaction Date"), c.category, c.category_group
),
category_stats AS (
    SELECT 
        category,
        category_group,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ABS(monthly_total)), 2) AS p50_monthly_sum,
        ROUND(PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY ABS(monthly_total)), 2) AS p85_monthly_sum,
        ROUND(AVG(ABS(monthly_total)), 2) AS avg_monthly_sum,
        ROUND(STDDEV_POP(ABS(monthly_total)), 2) AS stddev_monthly_sum,
        COUNT(*) FILTER (WHERE monthly_total != 0) AS months_with_spending
    FROM monthly_sums
    GROUP BY category, category_group
),
specified_month_sums AS (
    SELECT 
        c.category,
        c.category_group,
        COALESCE(ABS(SUM(ct.amount)), 0) AS specified_month_sum
    FROM category_list c
    LEFT JOIN consolidated_transactions ct 
        ON c.category = ct.category
        AND DATE_TRUNC('month', ct."Transaction Date") = (SELECT month FROM specified_month)
    GROUP BY c.category, c.category_group
)
SELECT 
    strftime('%B', sm.month) AS Month,
    strftime('%Y', sm.month) AS Year,
    cs.category,
    cs.category_group,
    cs.p50_monthly_sum,
    cs.p85_monthly_sum,
    cs.avg_monthly_sum,
    cs.stddev_monthly_sum,
    cs.months_with_spending,
    CASE 
        WHEN cs.avg_monthly_sum = 0 THEN NULL
        ELSE ROUND((cs.stddev_monthly_sum / cs.avg_monthly_sum) * 100, 2)
    END AS avg_percent_variance,
    ROUND(COALESCE(sms.specified_month_sum, 0), 2) AS specified_month_sum,
    cb.budget,
    CASE
        WHEN cb.budget IS NULL THEN ''
        WHEN sms.specified_month_sum < cb.budget THEN 'Under budget by $' || (cb.budget - sms.specified_month_sum)::INT::TEXT
        WHEN sms.specified_month_sum > cb.budget THEN 'Over budget by $' || (sms.specified_month_sum - cb.budget)::INT::TEXT
        ELSE 'On budget'
    END AS budget_status
FROM category_stats cs
LEFT JOIN specified_month_sums sms ON cs.category = sms.category
LEFT JOIN current_budgets cb ON cs.category = cb.category
CROSS JOIN specified_month sm
ORDER BY ROUND(COALESCE(sms.specified_month_sum, 0), 2) DESC;
