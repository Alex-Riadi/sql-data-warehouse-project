--checking for duplicates after joining
--SELECT cst_id, COUNT(*) FROM (
	SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid 
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid
--)t
--GROUP BY cst_id
--HAVING COUNT(*) != 1

--integrate 2 gender collumn while prioritizing source from crm
SELECT 
ci.cst_gndr,
ca.gen,
CASE WHEN ci.cst_gndr != ca.gen THEN ci.cst_gndr
	ELSE COALESCE(ca.gen, 'n/a')
END AS new_gender
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid 
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

--use friendly name
SELECT 
ci.cst_id				AS customer_id,
ci.cst_key				AS customer_number,
ci.cst_firstname		AS first_name,
ci.cst_lastname			AS last_name,
la.cntry				AS country,
ci.cst_marital_status	AS marital_status,
ca.bdate				AS birth_date,
ci.cst_create_date		AS create_date,
CASE	WHEN ci.cst_gndr != ca.gen THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'n/a')
END						AS gender
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca	
ON ci.cst_key = ca.cid 
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

--add surrogate key
SELECT 
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
ci.cst_id							AS customer_id,
ci.cst_key							AS customer_number,
ci.cst_firstname					AS first_name,
ci.cst_lastname						AS last_name,
la.cntry							AS country,
ci.cst_marital_status				AS marital_status,
ca.bdate							AS birth_date,
ci.cst_create_date					AS create_date,
CASE	WHEN ci.cst_gndr != ca.gen THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'n/a')
END									AS gender
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca	
ON ci.cst_key = ca.cid 
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

--============================================================
CREATE VIEW gold.dim_customers AS
	SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id							AS customer_id,
	ci.cst_key							AS customer_number,
	ci.cst_firstname					AS first_name,
	ci.cst_lastname						AS last_name,
	la.cntry							AS country,
	ci.cst_marital_status				AS marital_status,
	ca.bdate							AS birth_date,
	ci.cst_create_date					AS create_date,
	CASE	WHEN ci.cst_gndr != ca.gen THEN ci.cst_gndr
			ELSE COALESCE(ca.gen, 'n/a')
	END									AS gender
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca	
	ON ci.cst_key = ca.cid 
	LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid
--============================================================

CREATE VIEW gold.dim_products AS
	SELECT 
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id		AS product_id,
	pn.prd_key		AS product_number,
	pn.prd_nm		AS product_name,

	--pn.cat_id,
	pn.cat_id_2		AS category_id,
	pc.cat			AS category,
	pc.subcat		AS subcategory,
	pc.maintenance	AS maintenance,

	pn.prd_line		AS product_line,
	pn.prd_cost		AS product_cost,
	pn.prd_start_dt	AS product_start_date

	FROM silver.crm_prd_info pn
	LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id_2 = pc.id
	WHERE pn.prd_end_dt IS NULL

--============================================================
CREATE VIEW gold.fact_sales AS
	SELECT
	sd.sls_ord_num		AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt		AS order_date,
	sd.sls_ship_dt		AS shipping_date,
	sd.sls_due_dt		AS due_date,
	sd.sls_sales		AS sales_amount,
	sd.sls_quantity		AS quantity,
	sd.sls_price		AS price
	FROM silver.crm_sales_details sd
	LEFT JOIN gold.dim_products pr
	ON sd.sls_prd_key = pr.product_number
	LEFT JOIN gold.dim_customers cu
	ON sd.sls_cust_id = cu.customer_id

--========================================
--FOREIGN KEY CHECK

SELECT * FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON c.customer_key = s.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = s.product_key
WHERE c.customer_key IS NULL OR p.product_key IS NULL

