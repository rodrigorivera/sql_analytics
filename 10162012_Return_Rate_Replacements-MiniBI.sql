/* This one is to obtain the sales before returns */
(SELECT COUNT(DISTINCT id_sales_order_item) AS DC_id_sales_order_item
FROM AZMALO.sales_order_item
WHERE dwh_shipment_state NOT IN('item_invalid', 'item_canceled', 'item_rejected')
) br

/* This is an alternative for sales before returns better suited for joins */
define v_venture = 'namshi_ae'

SELECT SUM(br.sale) AS SALES_BEF_REV
FROM
(SELECT DISTINCT id_sales_order_item AS DC_id_sales_order_item, 1 AS sale
FROM &v_venture.sales_order_item
WHERE dwh_shipment_state NOT IN('item_invalid', 'item_canceled', 'item_rejected')
) br
;
/* This is an alternative for sales after returns better suited for joins */
(SELECT COUNT(DISTINCT id_sales_order_item) AS SOLD_ARTICLES
FROM sales_order_item
WHERE dwh_shipment_state NOT IN ('item_invalid','item_canceled','item_returned','item_exchanged')) ar



SELECT 
  pr.parent_id, 
  pr.entity_id, 
  pr.sku, 
  pr.type_id, 
  soi.sku, 
  so.created_at
FROM AZMALO.product pr
  LEFT JOIN AZMALO.sales_order_item soi 
    ON soi.sku=pr.sku
  LEFT JOIN AZMALO.sales_order so 
    ON soi.fk_sales_order = so.id_sales_order;


	SELECT p1.entity_id, p1.parent_id, p2.entity_id, p2.parent_id, p1.sku, p2.sku 
	FROM AZMALO.product p1
	  LEFT JOIN AZMALO.product p2
	    ON p1.parent_id = p2.entity_id
	
	
	/* CURRENT WORK*/
	
	SELECT
	sold_articles_bef_return, sold_articles

	FROM
	(SELECT 
	  p.parent_id,
	  p.entity_id, 
	  p.sku, 
	  p.type_id, 
	  soi.sku, 
	  so.created_at,
	  0 AS sold_articles_bef_return,
	  1 AS sold_articles
	FROM AZMALO.product pr
	  LEFT JOIN AZMALO.sales_order_item soi 
	    ON soi.sku=pr.sku
	  LEFT JOIN AZMALO.sales_order so 
	    ON soi.fk_sales_order = so.id_sales_order
	  WHERE
	  pr.parent_id IS NOT NULL
	  AND soi.dwh_shipment_state NOT IN('item_invalid', 'item_canceled', 'item_rejected')
	  UNION ALL
	SELECT 
	  pr.parent_id,
	  pr.entity_id, 
	  pr.sku, 
	  pr.type_id, 
	  soi.sku, 
	  so.created_at,
	  1 AS sold_articles_bef_return,
	  0 AS sold_articles
	FROM AZMALO.product pr
	  LEFT JOIN AZMALO.sales_order_item soi 
	    ON soi.sku=pr.sku
	  LEFT JOIN AZMALO.sales_order so 
	    ON soi.fk_sales_order = so.id_sales_order
	  WHERE
	  pr.parent_id IS NOT NULL
	  AND soi.dwh_shipment_state NOT IN('item_invalid','item_canceled','item_returned','item_exchanged'))
	  GROUP BY
	  pr.sku, 
	  ;
	
	/* CURRENT WORK 22.10.2012 */
	SELECT
		  r.name,
		  SUM(r.sbr) AS sold_articles_bef_return,
		  SUM(r.sar) AS sold_articles,
		  CASE 
								WHEN SUM(r.sbr) = 0 THEN 0
								ELSE (SUM(r.sbr) - SUM(r.sar))/SUM(r.sbr)
		  END AS return_rate_description
		FROM
		  (
		    SELECT
		      DISTINCT id_sales_order_item AS DC_id_sales_order_item,
		      sku,
	        fk_sales_order,
	        name,
		      1 AS sbr,
		      0 AS sar
		    FROM namshi_ae.sales_order_item
		    WHERE dwh_shipment_state NOT IN('item_invalid', 'item_canceled', 'item_rejected')
		UNION
		    SELECT
		      DISTINCT id_sales_order_item AS DC_id_sales_order_item,
		      sku,
	        fk_sales_order,
	        name,
		      0 AS sbr,
		      1 AS sar
		    FROM namshi_ae.sales_order_item
		    WHERE dwh_shipment_state NOT IN ('item_invalid','item_canceled','item_returned','item_exchanged')
		  )r
	  LEFT JOIN namshi_ae.sales_order so 
	    ON (so.id_sales_order = r.fk_sales_order)
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
		GROUP BY r.name;
		
		/* CÚRRENT WORK 23.10.2012 */
		
		/* This calculates Prodcut simple sku */
		SELECT
			  r.sku,
			  SUM(r.sbr) AS sold_articles_bef_return,
			  SUM(r.sar) AS sold_articles,
			  CASE 
									WHEN SUM(r.sbr) = 0 THEN 0
									ELSE (SUM(r.sbr) - SUM(r.sar))/SUM(r.sbr)
			  END AS return_rate_description
			FROM
			  (
			    SELECT
			      DISTINCT id_sales_order_item AS DC_id_sales_order_item,
			      sku,
		        fk_sales_order,
		        name,
			      1 AS sbr,
			      0 AS sar
			    FROM namshi_ae.sales_order_item
			    WHERE dwh_shipment_state NOT IN('item_invalid', 'item_canceled', 'item_rejected')
			UNION
			    SELECT
			      DISTINCT id_sales_order_item AS DC_id_sales_order_item,
			      sku,
		        fk_sales_order,
		        name,
			      0 AS sbr,
			      1 AS sar
			    FROM namshi_ae.sales_order_item
			    WHERE dwh_shipment_state NOT IN ('item_invalid','item_canceled','item_returned','item_exchanged')
			  )r
		  LEFT JOIN namshi_ae.sales_order so 
		    ON (so.id_sales_order = r.fk_sales_order)
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
			GROUP BY r.sku;
			
			
			/* This calculates the Returns per Product CONFIG SKU*/
		SELECT
			  r.productsku,
			  SUM(r.sbr) AS sold_articles_bef_return,
			  SUM(r.sar) AS sold_articles,
			  CASE 
									WHEN SUM(r.sbr) = 0 THEN 0
									ELSE (SUM(r.sbr) - SUM(r.sar))/SUM(r.sbr)
			  END AS return_rate_description
			FROM
			  (
			    SELECT
			      DISTINCT id_sales_order_item AS DC_id_sales_order_item,
			      sku,
		        fk_sales_order,
		        name,
			      1 AS sbr,
			      0 AS sar,
            SUBSTR(sku,0,INSTR(sku,'-')-1) AS productsku
			    FROM namshi_ae.sales_order_item
			    WHERE dwh_shipment_state NOT IN('item_invalid', 'item_canceled', 'item_rejected')
			UNION
			    SELECT
			      DISTINCT id_sales_order_item AS DC_id_sales_order_item,
			      sku,
		        fk_sales_order,
		        name,
			      0 AS sbr,
			      1 AS sar,
            SUBSTR(sku,0,INSTR(sku,'-')-1) AS productsku
			    FROM namshi_ae.sales_order_item
			    WHERE dwh_shipment_state NOT IN ('item_invalid','item_canceled','item_returned','item_exchanged')
			  )r
		  LEFT JOIN namshi_ae.sales_order so 
		    ON (so.id_sales_order = r.fk_sales_order)
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
			GROUP BY r.configsku

	  
	  
	  
	  /* This calculates Returns per Brand */ 
    SELECT
			  p.brand AS brand,
			  SUM(r.sbr) AS sold_articles_bef_return,
			  SUM(r.sar) AS sold_articles,
			  CASE 
									WHEN SUM(r.sbr) = 0 THEN 0
									ELSE (SUM(r.sbr) - SUM(r.sar))/SUM(r.sbr)
			  END AS return_rate_description
			FROM
			  (
			    SELECT
			      DISTINCT id_sales_order_item AS DC_id_sales_order_item,
			      sku,
		        fk_sales_order,
		        name,
			      1 AS sbr,
			      0 AS sar,
            SUBSTR(sku,0,INSTR(sku,'-')-1) AS productsku
			    FROM namshi_ae.sales_order_item
			    WHERE dwh_shipment_state NOT IN('item_invalid', 'item_canceled', 'item_rejected')
			UNION
			    SELECT
			      DISTINCT id_sales_order_item AS DC_id_sales_order_item,
			      sku,
		        fk_sales_order,
		        name,
			      0 AS sbr,
			      1 AS sar,
            SUBSTR(sku,0,INSTR(sku,'-')-1) AS productsku
			    FROM namshi_ae.sales_order_item
			    WHERE dwh_shipment_state NOT IN ('item_invalid','item_canceled','item_returned','item_exchanged')
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