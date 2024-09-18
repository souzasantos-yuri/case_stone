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

-- definindo o cohorte com a data truncada
def_activate_month AS (

    SELECT
		customer_id,
        MIN(transaction_date) AS first_purchase_date,
		DATETRUNC(MONTH, transaction_date) AS activate_date
    FROM def_filter
    GROUP BY 
		customer_id, transaction_date
),

-- criando a agrega��o das datas de transa��o e data de cohorte
def_agg_diff AS (

	SELECT 
		o.*,
		c.activate_date,
		YEAR(transaction_date) AS transaction_year,
		MONTH(transaction_date) AS transaction_month,
		YEAR(activate_date) AS activate_year,
		MONTH(activate_date) AS activate_month
	FROM def_filter AS o
	LEFT JOIN def_activate_month AS c
	ON o.customer_id = c.customer_id -- poss�vel utiliza��o da cl�usula USING em outros BDs como por exemplo BigQuery
	
),


-- criando o �ndice de cohorte
def_activate_index AS (

	SELECT
		def_agg_diff.*,
		(transaction_year - activate_year) * 12 + (transaction_month - activate_month) + 1 AS activate_index
	FROM def_agg_diff
),


-- preparando os dados para transpor as informa��es da tabela
def_pivot_data AS (

	SELECT DISTINCT 
		customer_id, 
		activate_date, 
		activate_index
	FROM def_activate_index
)

-- c�lculo final para plotar os gr�ficos pivotados
SELECT 

	LEFT(activate_date, 7) as activate_month, -- usando slice na data para trazer apenas o m�s
	CAST(1.0*[1]/[1]*100 AS DECIMAL(10, 2)) AS [1], -- usando os �ndices para comprar a diferen�a entre um �ndice e o outro
	CAST(1.0*[2]/[1]*100 AS DECIMAL(10, 2)) AS [2],
	CAST(1.0*[3]/[1]*100 AS DECIMAL(10, 2)) AS [3],
	CAST(1.0*[4]/[1]*100 AS DECIMAL(10, 2)) AS [4],
	CAST(1.0*[5]/[1]*100 AS DECIMAL(10, 2)) AS [5],
	CAST(1.0*[6]/[1]*100 AS DECIMAL(10, 2)) AS [6],
	CAST(1.0*[7]/[1]*100 AS DECIMAL(10, 2)) AS [7],
	CAST(1.0*[8]/[1]*100 AS DECIMAL(10, 2)) AS [8],
	CAST(1.0*[9]/[1]*100 AS DECIMAL(10, 2)) AS [9],
	CAST(1.0*[10]/[1]*100 AS DECIMAL(10, 2)) AS [10],
	CAST(1.0*[11]/[1]*100 AS DECIMAL(10, 2)) AS [11],
	CAST(1.0*[12]/[1]*100 AS DECIMAL(10, 2)) AS [12]

FROM def_pivot_data PIVOT (COUNT(customer_id) FOR activate_index IN ([1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12])) AS pivot_table
ORDER BY 1
