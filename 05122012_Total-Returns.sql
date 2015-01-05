/* Total Returns

Description:
Obtain Return rates without using PSQL

Version: v1 05 December 2012


Author: Rodrigo Rivera

Open Issues:
- TBD

*/

--*****************************************************************************************************************

SELECT
	--productsku,
	SUM (order_bef_cancel_flag) AS orders_bef_cancel
	,SUM (order_bef_return_flag) AS orders_bef_return
	,SUM (order_flag) AS orders
	,SUM (rev_bef_cancel_flag) AS sold_items_bef_cancel
	,SUM (rev_bef_return_flag) AS sold_items_bef_return
	,SUM (rev_flag) AS sold_items
	,SUM (customer_bef_cancel_flag) AS customers_bef_cancel
	,SUM (customer_bef_return_flag) AS customers_bef_return
	,SUM (customer_flag) AS customers
	,SUM (rev_bef_cancel_flag*paid_price) AS rev_bef_cancel
	,SUM (rev_bef_return_flag*paid_price) AS rev_bef_return
	,SUM (rev_flag*paid_price) AS rev
	,CASE
		WHEN SUM (rev_bef_return_flag) = 0 THEN 0
		ELSE ROUND((SUM (rev_bef_return_flag) - SUM (rev_flag))/SUM (rev_bef_return_flag),3)
	END AS return_rate_item_based
	,CASE
		WHEN SUM (rev_bef_return_flag*paid_price) = 0 THEN 0
		ELSE ROUND((SUM (rev_bef_return_flag*paid_price) - SUM (rev_flag*paid_price))/SUM (rev_bef_return_flag*paid_price),3)
	END AS return_rate_rev_based
FROM
(
SELECT
		o.mage_customer_entity_id AS bob_customer_id
		,o.created_at AS bob_order_time
		,oi.id_sales_order_item
		,oi.dwh_shipment_state
		,oi.paid_price
		,oi.sku
		,shoes_config.sku AS productsku
		,ROW_NUMBER () OVER (PARTITION BY oi.ID_SALES_ORDER_ITEM ORDER BY oi.fk_sales_order)AS no_rep
		,CASE
			WHEN upper(cd.campaign) LIKE '%ABANDONED%' THEN 1
			ELSE 0
		END AS abandoned
		,CASE
			WHEN dwh_shipment_state NOT IN ('item_invalid') THEN 1
			ELSE 0
		END AS rev_bef_cancel_flag
		,CASE
			WHEN dwh_shipment_state NOT IN ('item_invalid','item_canceled') THEN 1
			ELSE 0
		END AS rev_bef_return_flag
		,CASE
			WHEN dwh_shipment_state NOT IN ('item_invalid','item_canceled','item_returned','item_rejected','item_exchanged') THEN 1
			ELSE 0
		END AS rev_flag
		,CASE
			WHEN row_number () over (partition by order_nr order by id_sales_order_item) = 1 AND dwh_shipment_state NOT IN ('item_invalid') THEN 1
			ELSE 0
		END AS order_bef_cancel_flag
		,CASE
			WHEN row_number () over (partition by order_nr order by id_sales_order_item) = 1 AND dwh_shipment_state NOT IN ('item_invalid','item_canceled') THEN 1
		ELSE 0
		END AS order_bef_return_flag
		,CASE
			WHEN row_number () over (partition by order_nr order by id_sales_order_item) = 1 AND dwh_shipment_state NOT IN ('item_invalid','item_canceled','item_returned','item_rejected','item_exchanged') THEN 1
			ELSE 0
		END AS order_flag
		,CASE
			WHEN row_number () over (partition by mage_customer_entity_id order by id_sales_order_item) = 1 AND dwh_shipment_state NOT IN ('item_invalid') THEN 1
			ELSE 0
		END AS customer_bef_cancel_flag
		,CASE
			WHEN row_number () over (partition by mage_customer_entity_id order by id_sales_order_item) = 1 AND dwh_shipment_state NOT IN ('item_invalid','item_canceled') THEN 1
			ELSE 0
		END AS customer_bef_return_flag
		,CASE
			WHEN row_number () over (partition by mage_customer_entity_id order by id_sales_order_item) = 1 AND dwh_shipment_state NOT IN ('item_invalid','item_canceled','item_returned','item_rejected','item_exchanged') THEN 1
			ELSE 0
		END AS customer_flag
	FROM
		/* ************ GET ORDER ITEM ********* */
		(
			SELECT
				id_sales_order_item
				,dwh_shipment_state
				,paid_price
				,sku
				,fk_sales_order
			FROM
				rocket_dwh_dl.DG_BOB_SALES_ORDER_ITEM
				,(
					SELECT
						MAX(created_at) - 30 AS min30
						,MAX(created_at) - 150 AS max150
					FROM rocket_dwh_dl.dg_bob_sales_order_item
				) max_min_created
				WHERE
					TRUNC(CREATED_AT) < max_min_created.min30
					AND TRUNC(CREATED_AT)> max_min_created.max150
					AND DG_END_DATE>SYSDATE
		)oi
		/* ************ GET ORDER ********* */
		JOIN
			(
				SELECT
					mage_customer_entity_id
					,id_sales_order
					,order_nr
					,created_at
				FROM
					rocket_dwh_dl.DG_BOB_SALES_ORDER
				WHERE
					DG_END_DATE > SYSDATE
			)o
			ON oi.fk_sales_order = o.id_sales_order
		 /* ************ GET SIMPLE SKU ********* */
		JOIN
			(
				SELECT
					sku
					,type_id
					,parent_id
				FROM
					rocket_dwh_dl.dg_mag_ctlg_prdct_shoes
				WHERE
					type_id = 'simple'
					AND dg_end_date > sysdate
			)shoes
			ON shoes.sku = oi.sku
		/* ************ GET CONFIG SKU ********* */
		JOIN
			(
				SELECT
					sku
					,type_id
					,entity_id
				FROM
					rocket_dwh_dl.dg_mag_ctlg_prdct_shoes
				WHERE
					type_id = 'configurable'
					AND dg_end_date > sysdate
			)shoes_config
			ON shoes.parent_id = shoes_config.entity_id
		/* *************** GET FULL ORDERS SID ******* */
		JOIN
			(
				SELECT
					order_id
					,sid
				FROM
					rocket_dwh_dl.sta_wtr_getfullorders
			)gfo
			ON gfo.order_id = o.order_nr
		/* *************** GET FULL VISITORS SID ******* */
		JOIN
			(
				SELECT
					sid
				FROM
					rocket_dwh_dl.sta_wtr_getfullvisitors
			)gfv
			ON gfo.sid = gfv.sid
		/* *************** GET FULL CAMPAIGNS SID ******* */
	 	JOIN
			(
				SELECT
					sid
					,campaign
				FROM
					rocket_dwh_dl.sta_wtr_getfullcampaigns
			)gfc
			ON gfc.sid = gfv.sid
		/* *************** GET CAMPAIGN ******* */
		JOIN
			(
				SELECT
					datasourcevalue
					,campaign
				FROM
					rocket_dwh_dl.sta_wtr_confdata
			)cd
			ON gfc.campaign = cd.datasourcevalue
		WHERE 1=1
)
	WHERE abandoned = 1 AND no_rep=1
--GROUP BY productsku
--ORDER return_rate_item_based DESC
;
