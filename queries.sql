
with percentiles as (
    WITH monthly_sums AS (
        SELECT 
            "Category",
            DATE_TRUNC('month', "Transaction Date") AS month,
            abs(SUM("Amount")) AS monthly_total
        FROM 
            consolidated_transactions
        GROUP BY 
            "Category", DATE_TRUNC('month', "Transaction Date")
    )
    SELECT 
        "Category",
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY monthly_total) AS p50_monthly_sum,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY monthly_total) AS p90_monthly_sum
    FROM 
        monthly_sums
    GROUP BY 
        "Category"
    ORDER BY 
        "Category"
)
select ct.Category, sum(amount)*-1 as subtotal, p50_monthly_sum, p90_monthly_sum
from consolidated_transactions ct 
join percentiles p on ct.category=p.category
where "Transaction Date" > '2024-07-01' and ct.category is not null
group by 1, 3, 4 
having sum(amount) < 0
order by 2 desc;

