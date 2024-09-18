--filtrando valores deduplicados
WITH def_filter AS (

SELECT * FROM (

		SELECT 
			*, 
			ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY transaction_date DESC) AS rnk
		FROM dbo.transactions
		WHERE transaction_amount > 0 

	) AS filter

	WHERE rnk = 1 -- filtrando apenas a row_number = 1. No BigQuery seria possível omitir essa CTE utilizando a cláusula QUALIFY
),

def_rolling_tpv AS (

SELECT
	*,
	SUM(transaction_amount) OVER(PARTITION BY transaction_date ORDER BY transaction_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_tpv_7
FROM def_filter


),

def_test AS ( 

SELECT 
	*,
	COUNT(transaction_id) OVER(PARTITION BY customer_id ORDER BY transaction_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS active_costumers_7
FROM def_rolling_tpv

)

SELECT
	transaction_date AS ref_date,
	SUM(rolling_tpv_7) AS rolling_tpv_7,
	SUM(active_costumers_7) AS active_costumers_7
FROM def_test
GROUP BY transaction_date
ORDER BY 1