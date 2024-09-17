--recuperando valores deduplicados
WITH online_retail_clean AS ( 

        SELECT *, ROW_NUMBER() OVER(PARTITION BY transaction_id, customer_id ORDER BY transaction_date DESC) AS rnk
        FROM dbo.transactions
        WHERE transaction_amount > 0 
),

-- criando cohorte e achando a primeira compra
cohort AS (

    SELECT 
        customer_id,
        MIN(transaction_date) AS first_purchase_date,
        DATETRUNC(MONTH, transaction_date) AS cohort_date
    FROM online_retail_clean
	WHERE rnk = 1
    GROUP BY 
		customer_id,
		DATETRUNC(MONTH, transaction_date)
),

agg_diff AS (

select 
    o.*,
    c.cohort_date,
    YEAR(transaction_date) AS transaction_year,
    MONTH(transaction_date) AS transaction_month,
    YEAR(cohort_date) AS cohort_year,
    MONTH(cohort_date) AS cohort_month
from online_retail_clean AS o
left join cohort AS c
on o.customer_id = c.customer_id

),

calculate_diff AS (

SELECT 
    agg_diff.*,
    DATEDIFF(year, transaction_year, cohort_year) AS year_diff,
    DATEDIFF(month, transaction_month, cohort_month) AS month_diff
FROM agg_diff

),

cohort_retention AS (

SELECT
    calculate_diff.*,
    year_diff * 12 + month_diff + 1 AS cohort_index
FROM
    calculate_diff
)

SELECT * from cohort_retention