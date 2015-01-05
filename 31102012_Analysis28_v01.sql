SELECT
r.sku AS productsku,

			SUM(r.sbr) AS sold_products_bef_return,
			SUM(r.sar) AS sold_products_aft_return,
			CASE 
				WHEN SUM(r.sbr) = 0 THEN 0
				ELSE (SUM(r.sbr) - SUM(r.sar))/SUM(r.sbr)
			END AS return_rate_product
FROM
(
	SELECT
		id_sales_order_item AS DC_id_sales_order_item,
		soi.sku AS sku,
		soi.fk_sales_order,
		soi.name,
	1 AS sbr,
	0 AS sar,
	0 AS sbc,
	0 AS sac,
	SUBSTR(sku,0,INSTR(sku,'-')-1) AS productsku
  FROM dg_bob_sales_order_item soi
	LEFT JOIN dg_bob_sales_order so 
		ON soi.fk_sales_order = so.id_sales_order
	LEFT JOIN sta_wtr_getfullorders gfo
		 ON gfo.order_id = so.order_nr
	LEFT JOIN  sta_wtr_getfullcampaigns gfc
		ON gfc.sid = gfo.sid
	LEFT JOIN dg_wtr_confdata cd
		ON gfc.campaign = cd.datasourcevalue
WHERE 
	dwh_shipment_state NOT IN('item_invalid', 'item_canceled')
	AND
	upper(cd.category_4th_level) LIKE '%ABANDONED%'
	AND so.created_at >= '01-AUG-12'
	AND so.created_at <= '30-AUG-12'
UNION
SELECT
		id_sales_order_item AS DC_id_sales_order_item,
		soi.sku AS sku,
		soi.fk_sales_order,
		soi.name,
	0 AS sbr,
	1 AS sar,
	0 AS sbc,
	0 AS sac,
	SUBSTR(sku,0,INSTR(sku,'-')-1) AS productsku
  FROM dg_bob_sales_order_item soi
	LEFT JOIN dg_bob_sales_order so 
		ON soi.fk_sales_order = so.id_sales_order
	LEFT JOIN sta_wtr_getfullorders gfo
		 ON gfo.order_id = so.order_nr
	LEFT JOIN  sta_wtr_getfullcampaigns gfc
		ON gfc.sid = gfo.sid
	LEFT JOIN dg_wtr_confdata cd
		ON gfc.campaign = cd.datasourcevalue
WHERE 
	dwh_shipment_state NOT IN ('item_invalid','item_canceled','item_rejected','item_returned','item_exchanged')
	AND
	upper(cd.category_4th_level) LIKE '%ABANDONED%'
	AND so.created_at >= '01-AUG-12'
	AND so.created_at <= '30-AUG-12'
	)r
GROUP BY r.sku