--filtrando valores deduplicados
WITH def_filter AS (

SELECT * FROM (

		SELECT 
			*, 
			ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY transaction_date DESC) AS rnk
		FROM dbo.transactions
		WHERE transaction_amount > 0 

	) AS filter

	WHERE rnk = 1 -- filtrando apenas a row_number = 1. No BigQuery seria poss�vel omitir essa CTE utilizando a cl�usula QUALIFY
),

-- usando o max pra definir a compra mais recente dado um consumidor

def_last_purchase AS (

SELECT
	customer_id,
	MAX(transaction_date) AS last_purchase,
	transaction_date
FROM def_filter
GROUP BY
	customer_id, transaction_date
)

-- usando EOMONTH que serve como um DATETRUNC inverso para pegar o ultimo dia do mês
SELECT
	EOMONTH(transaction_date) AS reference_month,
	COUNT(DISTINCT customer_id) AS churned_customers
FROM def_last_purchase
WHERE DATEDIFF(DAY, last_purchase, EOMONTH(transaction_date)) > 28 -- fazendo a diferença da compra mais recente com a data do fim do mês e retornando os usuários que não compraram nos ultimos 28 dias
GROUP BY transaction_date