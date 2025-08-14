IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products
GO
CREATE VIEW gold.dim_products AS 
SELECT 
	ROW_NUMBER() OVER (ORDER BY pr.prd_start_dt, pr.prd_key) AS product_key,
	pr.prd_id AS product_id,
	pr.prd_key AS product_number,
	pr.prd_nm AS product_name,
	pr.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance AS mainyenance,
	pr.prd_cost AS cost,
	pr.prd_line AS product_line,
	pr.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pr
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
ON pr.cat_id = pc.id
WHERE pr.prd_end_dt IS NULL

GO

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers
GO
CREATE VIEW gold.dim_customers AS 
SELECT 
	ROW_NUMBER() OVER (ORDER BY cu.cst_id) AS customer_key,
	cu.cst_id AS customer_id,
	cu.cst_key AS customer_number,
	cu.cst_firstname AS first_name,
	cu.cst_lastname AS last_name,
	la.cntry AS country,
	CASE WHEN cu.cst_gender != 'n/a' THEN cu.cst_gender
		ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	ca.bdate AS birthday,
	cu.cst_material_status AS marital_status,
	cu.cst_create_date AS create_date
FROM silver.crm_cust_info AS cu
LEFT JOIN silver.erp_cust_az12 ca
ON cu.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON  cu.cst_key = la.cid

GO

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales
GO
CREATE VIEW gold.fact_sales AS 
SELECT 
	sls_ord_num,
	pr.product_key,
	cu.customer_key,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cst_id = cu.customer_id


