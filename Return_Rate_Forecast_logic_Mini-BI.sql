/* Return Rate Forecast per Product SKU*/
/* Required Tables in DWH:
DIM_DATES AS d
DIM_ARTICLES AS a
DIM_PRODUCTS AS p
FCT_SALES AS s
*/
/* Required columns (from DWH to MINI BI)
DIM_ARTICLES.ARTICLE_ID
DIM_DATES.ISO_DATE
DIM_DATES.DATE_ID
FCT_SALES.DATE_ID
FCT_SALES.SOLD_ARTICLES
FCT_SALES.SOLD_ARTICLES_BEF_RETURN
DIM_PRODUCTS.DESCRIPTION -> PRODUCT.NAME
DIM_PRODUCTS.PRODUCT_ID -> STOCK.ID_STOCK
DIM_PRODUCTS.TARGETGROUP
DIM_PRODUCTS.VALID_TO
DIM_PRODUCTS.VALID_FROM -> STOCK.STOCK_DATE // PRODUCT.ENABLED_AT // PRODUCT.CREATED_AT // PRODUCT.ACTIVATED_AT
DIM_PRODUCTS.SKU -> STOCK.SKU // PRODUCT.SKU
*/
SELECT 
	Product_SKU,
	targetgroup,
	description,
/* If SKU has been sold at least 8 times take return rate of SKU. If not take Return Rate of hierarchy level above. If there are
less than 8 sales on this hierarchy level before return the take return rate of highest level*/
	CASE 
		WHEN sold_articles_bef_return >= 8 THEN ROUND(return_rate_product,2)
		WHEN sold_articles_bef_return < 8 AND sold_articles_bef_return_tgt >= 8 THEN ROUND(return_rate_targetgroup,2)
		ELSE ROUND(return_rate_description,2)
	END AS return_rate_forecast
	--sold_articles_bef_return,
	--sold_articles,
	--sold_articles_bef_return_tgt,
	--sold_articles_tgt,
	--return_rate_product,
	--return_rate_targetgroup,
	--return_rate_description
FROM
	(
----------------------------------------------------
/* Calculate historical return rates per SKU*/
		SELECT 
			agg_product.Product_SKU,
			--agg_product."SUBTYPE",
			--agg_product."TYPE",
			agg_product.targetgroup,
			agg_product.description,
			agg_product.sold_articles_bef_return,
			agg_product.sold_articles,
			agg_targetgroup.sold_articles_bef_return AS sold_articles_bef_return_tgt,
			agg_targetgroup.sold_articles AS sold_articles_tgt,
			CASE 
				WHEN agg_product.sold_articles_bef_return = 0 THEN 0
				ELSE (agg_product.sold_articles_bef_return - agg_product.sold_articles) / agg_product.sold_articles_bef_return
			END AS return_rate_product,
			agg_targetgroup.return_rate_targetgroup,
			agg_description.return_rate_description
		FROM
			(
/* Join targetgroup to sales per Product SKU*/
				SELECT 
					Product_SKU,
					targetgroup,
					description,
					sold_articles_bef_return,
					sold_articles
				FROM
					(
/* Get Sales before and after returns per Product SKU*/
						SELECT
							p.sku AS Product_SKU,
							SUM(s.sold_articles_bef_return) AS sold_articles_bef_return,
							SUM(s.sold_articles) AS sold_articles
						FROM 
							fct_sales s
							JOIN dim_dates d ON s.date_id = d.date_id
							JOIN dim_articles a ON s.article_id = a.article_id
							JOIN dim_products p ON a.product_id = p.product_id
/* Only look at time period with length 48 days which is 30 days ago. The 30 days are chosen based on the assumption
that most returns will happen within 30 days after the order creation date. This is a Zalando assumption and should
be adjusted based on the local requirements. The 48 days have been chosen semi-randomly. It should reflect a time period
which is long enough to be representative and not too long to be too heavily affected by seasonalities and external effects*/
/* MAYBE HERE YOU DO NOT NEED TO USE DIM_DATES AND JUST USE THE DATE OF SALE */
						WHERE 
							d.date_id < (
											SELECT max(s.date_id) -30 
											FROM fct_sales s
										)
						AND 
							d.date_id >= (
											SELECT max(s.date_id) -78 
											FROM fct_sales s
										)
						GROUP BY
/* Group by must only be on one granularity level since each Product can historically be in multiple categories, thus creating
multiple datasets*/
							p.sku 
					) a
					JOIN dim_products p ON a.product_sku = p.sku
/* Improvement in v3: set max date of the observation period between valid_from and valid_to of the Product. Thus we join 
the categories to the SKU that were valid at the time of the end of the observation period and we avoid that the SKU can be 
in multiple categories over time which would generate multiple rows */
/* MAYBE THIS ONE CAN BE OPTIMIZED, IT SEEMS IT IS DUPLICATED, MAYBE IN JOIN OF LINE 101 */
				WHERE 
					(
						SELECT 
							to_date(iso_date, 'YYYY.MM.DD')
						FROM
							(
								SELECT max(s.date_id) -30 AS max_date_id
								FROM fct_sales s
							) a
							JOIN dim_dates d ON a.max_date_id = d.date_id
					) BETWEEN p.valid_from AND p.valid_to  
			) agg_product
/* We want to use return rates on Product SKU level if at least 8 items have been sold. If less items have been sold we are taking 
the return rate from the hierarchy above - in this case targetgroup. If in this hierarchy level we don't have 8 items as well
we move up one more level until we have reached the highest granularity level 
HERE WE ARE JOINING TARGETGROUP LEVEL*/
-----------------------------------------------------
			JOIN 
			(
				SELECT 
					targetgroup,
					sold_articles_bef_return,
					sold_articles,
					CASE 
						WHEN sold_articles_bef_return = 0 THEN 0
						ELSE (sold_articles_bef_return - sold_articles) / sold_articles_bef_return
					END AS return_rate_targetgroup
				FROM
					(
						SELECT
							p.targetgroup,
							SUM(s.sold_articles_bef_return) AS sold_articles_bef_return,
							SUM(s.sold_articles) AS sold_articles
						FROM 
							fct_sales s
							JOIN dim_dates d ON s.date_id = d.date_id
							JOIN dim_articles a ON s.article_id = a.article_id
							JOIN dim_products p ON a.product_id = p.product_id
/* Only look at time period with length 48 days which is 30 days ago. The 30 days are chosen based on the assumption
that most returns will happen within 30 days after the order creation date. This is a Zalando assumption and should
be adjusted based on the local requirements. The 48 days have been chosen semi-randomly. It should reflect a time period
which is long enough to be representative and not too long to be too heavily affected by seasonalities and external effects*/
						WHERE 
							d.date_id < (
											SELECT MAX(s.date_id) -30 
											FROM fct_sales s
										)
							AND d.date_id >= (
											SELECT MAX(s.date_id) -78 
											FROM fct_sales s
										)
						GROUP BY 
							p.targetgroup
					)
			) agg_targetgroup ON agg_product.targetgroup = agg_targetgroup.targetgroup
/* We want to use return rates on Product SKU level if at least 8 items have been sold. If less items have been sold we are taking 
the return rate from the hierarchy above - in this case targetgroup. If in this hierarchy level we don't have 8 items as well
we move up one more level until we have reached the highest granularity level 
HERE WE ARE JOINING DESCRIPTION LEVEL*/
-----------------------------------------------------
			JOIN
			(
				SELECT 
					description,
					sold_articles_bef_return,
					sold_articles,
					CASE 
						WHEN sold_articles_bef_return = 0 THEN 0
						ELSE (sold_articles_bef_return - sold_articles) / sold_articles_bef_return
						END AS return_rate_description
				FROM
					(
						SELECT
							p.description,
							SUM(s.sold_articles_bef_return) AS sold_articles_bef_return,
							SUM(s.sold_articles) AS sold_articles
						FROM 
							fct_sales s
							JOIN dim_dates d ON s.date_id = d.date_id
							JOIN dim_articles a ON s.article_id = a.article_id
							JOIN dim_products p ON a.product_id = p.product_id
/* Only look at time period with length 48 days which is 30 days ago. The 30 days are chosen based on the assumption
that most returns will happen within 30 days after the order creation date. This is a Zalando assumption and should
be adjusted based on the local requirements. The 48 days have been chosen semi-randomly. It should reflect a time period
which is long enough to be representative and not too long to be too heavily affected by seasonalities and external effects*/
						WHERE 
							d.date_id < (
											SELECT MAX(s.date_id) -30 
											FROM fct_sales s
										)
							AND d.date_id >= (
											SELECT MAX(s.date_id) -78 
											FROM fct_sales s
										)
						GROUP BY 
							p.description
					)
			) agg_description ON agg_product.description = agg_description.description
	)
;

