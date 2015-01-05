SELECT
			p.brand AS brand,
			SUM(r.sbr) AS sold_brands_bef_return,
			SUM(r.sar) AS sold_brands_aft_return,
			CASE 
				WHEN SUM(r.sbr) = 0 THEN 0
				ELSE ROUND((SUM(r.sbr) - SUM(r.sar))/SUM(r.sbr),2)
			END AS return_rate_brand,
			SUM(r.sbc) AS sold_brands_bef_cancellation,
			SUM(r.sac) AS sold_brands_aft_cancellation,
			CASE 
				WHEN SUM(r.sbc) = 0 THEN 0
				ELSE ROUND((SUM(r.sbc) - SUM(r.sac))/SUM(r.sbc),2)
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
      HAVING SUM(r.sbr) >= 100