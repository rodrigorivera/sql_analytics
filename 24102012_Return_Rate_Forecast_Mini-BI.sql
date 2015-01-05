
SELECT
	rcrf.pps AS sku,
	rcrf.pb AS brand,
	CASE
 /* If more then 8 products we take PRODUCT CONFIG for RETURNS*/
		WHEN rcrf.spbr >= 8 THEN ROUND(rcrf.rrp,2)
 /* If lass than 8 products we take BRAND */
		WHEN rcrf.spbr < 8 AND rcrf.sbbr >= 8 THEN ROUND(rcrf.rrb,2)
 /* Else we take ALL */
		ELSE ROUND(rcrf.all_rr,2)
	END AS return_rate_forecast,
 /* For CANCELLATIONS we take global*/
	ROUND(rcrf.all_cr,2) AS cancellation_rate_forecast
/* This is the main body of the script, it calculates separetely the return and cancellation rates of simples, products and brand */
FROM
(
SELECT
	product.productsku AS pps,
	product.brand AS pb,
	product.sold_products_bef_return AS spbr,
	product.sold_products_aft_return AS spar,
	product.return_rate_product AS rrp,
	product.sold_products_bef_cancellation AS spbc,
	product.sold_products_aft_cancellation AS spac,
	product.cancellation_rate_product AS crp,
	brand.sold_brands_bef_return AS sbbr,
	brand.sold_brands_aft_return AS sbar,
	brand.return_rate_brand AS rrb,
	brand.sold_brands_bef_cancellation AS sbbc,
	brand.sold_brands_aft_cancellation AS sbac,
	brand.cancellation_rate_brand AS crb
	,1 AS all_cr
	,1 AS all_rr
/*	,SUM(brand.sold_brands_bef_return) AS all_spbc,
	SUM(product.sold_products_aft_cancellation) AS all_spac,
	CASE 
		WHEN SUM(brand.sold_brands_bef_return) = 0 THEN 0
		ELSE (SUM(brand.sold_brands_bef_return) - SUM(product.sold_products_aft_cancellation))/SUM(brand.sold_brands_bef_return)
	END AS all_rr,
	CASE 
		WHEN SUM(brand.sold_brands_bef_cancellation) = 0 THEN 0
		ELSE (SUM(brand.sold_brands_bef_cancellation) - SUM(brand.sold_brands_aft_cancellation))/SUM(brand.sold_brands_bef_cancellation)
	END AS all_cr
*/
FROM
	(
/* ******************* PRODUCT CONFIG ******************************* */
/* This calculates the Returns and cancellations per Product CONFIG SKU*/
		SELECT
			r.productsku AS productsku,
			p.brand AS brand,
			SUM(r.sbr) AS sold_products_bef_return,
			SUM(r.sar) AS sold_products_aft_return,
			CASE 
				WHEN SUM(r.sbr) = 0 THEN 0
				ELSE (SUM(r.sbr) - SUM(r.sar))/SUM(r.sbr)
			END AS return_rate_product,
			SUM(r.sbc) AS sold_products_bef_cancellation,
			SUM(r.sac) AS sold_products_aft_cancellation,
			CASE 
				WHEN SUM(r.sbc) = 0 THEN 0
				ELSE (SUM(r.sbc) - SUM(r.sac))/SUM(r.sbc)
			END AS cancellation_rate_product
		FROM
			  (
/* Here we obtain the sold units BEFORE RETURN per PRODUCT CONFIG */
			    SELECT
					DISTINCT id_sales_order_item AS DC_id_sales_order_item,
					sku,
					fk_sales_order,
					name,
					1 AS sbr,
					0 AS sar,
					0 AS sbc,
					0 AS sac,
					SUBSTR(sku,0,INSTR(sku,'-')-1) AS productsku
			    FROM namshi_ae.sales_order_item
			    WHERE dwh_shipment_state NOT IN('item_invalid', 'item_canceled', 'item_rejected')
			UNION
/* Here we obtain the sold units AFTER RETURN per PRODUCT CONFIG */
			    SELECT
					DISTINCT id_sales_order_item AS DC_id_sales_order_item,
					sku,
					fk_sales_order,
					name,
					0 AS sbr,
					1 AS sar,
					0 AS sbc,
					0 AS sac,
					SUBSTR(sku,0,INSTR(sku,'-')-1) AS productsku
			    FROM namshi_ae.sales_order_item
			    WHERE dwh_shipment_state NOT IN ('item_invalid','item_canceled','item_rejected','item_returned','item_exchanged')
			UNION
/* Here we obtain the sold units BEFORE CANCELLATION per Product CONFIG */
			    SELECT
					DISTINCT id_sales_order_item AS DC_id_sales_order_item,
					sku,
					fk_sales_order,
					name,
					0 AS sbr,
					0 AS sar,
					1 AS sbc,
					0 AS sac,
					SUBSTR(sku,0,INSTR(sku,'-')-1) AS productsku
			    FROM namshi_ae.sales_order_item
			    WHERE dwh_shipment_state NOT IN('item_invalid')
			UNION
/* Here we obtain the sold units AFTER CANCELLATION per PRODUCT CONFIG */
			    SELECT
					DISTINCT id_sales_order_item AS DC_id_sales_order_item,
					sku,
					fk_sales_order,
					name,
					0 AS sbr,
					0 AS sar,
					0 AS sbc,
					1 AS sac,
					SUBSTR(sku,0,INSTR(sku,'-')-1) AS productsku
			    FROM namshi_ae.sales_order_item
			    WHERE dwh_shipment_state NOT IN ('item_invalid','item_canceled','item_exchanged')
			)r
			LEFT JOIN namshi_ae.sales_order so 
				ON (so.id_sales_order = r.fk_sales_order)
			LEFT JOIN namshi_ae.product p 
				ON p.sku = r.productsku
		  WHERE
		  (
		    so.created_at < (
		                      SELECT MAX(so.created_at)-30
		                      FROM namshi_ae.sales_order so
		                    )
		  AND so.created_at > (
		                      SELECT MAX(so.created_at) -78
		                      FROM namshi_ae.sales_order so
		                      )
		  )
			GROUP BY r.productsku, p.brand
	)product
/* ****************************** BRAND ************************** */
 LEFT JOIN
  /* This calculates the Returns and cancellations per BRAND */
	(
		SELECT
			p.brand AS brand,
			SUM(r.sbr) AS sold_brands_bef_return,
			SUM(r.sar) AS sold_brands_aft_return,
			CASE 
				WHEN SUM(r.sbr) = 0 THEN 0
				ELSE (SUM(r.sbr) - SUM(r.sar))/SUM(r.sbr)
			END AS return_rate_brand,
			SUM(r.sbc) AS sold_brands_bef_cancellation,
			SUM(r.sac) AS sold_brands_aft_cancellation,
			CASE 
				WHEN SUM(r.sbc) = 0 THEN 0
				ELSE (SUM(r.sbc) - SUM(r.sac))/SUM(r.sbc)
			END AS cancellation_rate_brand
		FROM
			  (
/* Here we obtain sold units BEFORE RETURN per BRAND */
			    SELECT
					DISTINCT id_sales_order_item AS DC_id_sales_order_item,
					sku,
					fk_sales_order,
					name,
					1 AS sbr,
					0 AS sar,
					0 AS sbc,
					0 AS sac,
					SUBSTR(sku,0,INSTR(sku,'-')-1) AS productsku
			    FROM namshi_ae.sales_order_item
			    WHERE dwh_shipment_state NOT IN('item_invalid', 'item_canceled', 'item_rejected')
			UNION
/* Here we obtain the sold units after Return per brand */
			    SELECT
					DISTINCT id_sales_order_item AS DC_id_sales_order_item,
					sku,
					fk_sales_order,
					name,
					0 AS sbr,
					1 AS sar,
					0 AS sbc,
					0 AS sac,
					SUBSTR(sku,0,INSTR(sku,'-')-1) AS productsku
			    FROM namshi_ae.sales_order_item
			    WHERE dwh_shipment_state NOT IN ('item_invalid','item_canceled','item_rejected','item_returned','item_exchanged')
			UNION
/* Here we obtain the sold units before Cancellation per brand */
			    SELECT
					DISTINCT id_sales_order_item AS DC_id_sales_order_item,
					sku,
					fk_sales_order,
					name,
					0 AS sbr,
					0 AS sar,
					1 AS sbc,
					0 AS sac,
					SUBSTR(sku,0,INSTR(sku,'-')-1) AS productsku
			    FROM namshi_ae.sales_order_item
			    WHERE dwh_shipment_state NOT IN('item_invalid')
			UNION
/* Here we obtain the sold units after Cancellation per brand */
			    SELECT
					DISTINCT id_sales_order_item AS DC_id_sales_order_item,
					sku,
					fk_sales_order,
					name,
					0 AS sbr,
					0 AS sar,
					0 AS sbc,
					1 AS sac,
					SUBSTR(sku,0,INSTR(sku,'-')-1) AS productsku
			    FROM namshi_ae.sales_order_item
			    WHERE dwh_shipment_state NOT IN ('item_invalid','item_canceled','item_exchanged')
			  )r
		  LEFT JOIN namshi_ae.sales_order so 
		    ON (so.id_sales_order = r.fk_sales_order)
      LEFT JOIN
      namshi_ae.product p ON p.sku= r.productsku
		  WHERE
		  (
		    so.created_at < (
		                      SELECT MAX(so.created_at)-30
		                      FROM namshi_ae.sales_order so
		                    )
		  AND so.created_at > (
		                      SELECT MAX(so.created_at) -78
		                      FROM namshi_ae.sales_order so
		                      )
		  )
      
			GROUP BY p.brand
	)brand ON product.brand = brand.brand
 )rcrf