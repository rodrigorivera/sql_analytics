/* RETURN AND CANCELLATION RATE FORECAST

Description:
Ventures face the problem that cancellation rates AND return rates for the recent period are not known. This analysis
predicts returns AND cancellations ON a config SKU level based ON historical return AND cancellation behavior.

Version: v1.1, 2012-October-29

Version history:
- v1.0 initial version
- v1.1 initial version was ON simple SKU granularity. Adjusted to config granularity. Also added "DISTINCT" to
product catalogue to make sure that sales are not counted multiple times WHEN there is more than one entry per SKU
in the catalogue

Author: Rodrigo Rivera

Open Issues:
- WHEN BOB provides unique categories per SKU we can also use categories for return rate proxies (v2.0)

*/

/* **************************************************************************************************************** */


SET SERVEROUTPUT ON

DECLARE

V_MINIMUM_SALES NUMBER := 30; -- WHEN there is not a significant number ON sales ON each level, i.e. sales are < than this number, the return rate of a higher level is used. Example: config SKU-4711 of brand Samsung has 15 sales, but Samsung has 80 sales. In this CASE the return rate is taken FROM Samsung. Zalando uses 8, but analysis at Jabong has shown that at least 30 items are necessary that return rates are comparable over different periods.
/* V_VENTURE VARCHAR2(100) := 'namshi_ae'; -- this is the schema name of the venture */
V_TRAINING_PERIOD_END NUMBER := 30; -- this is the END point of the training period in days FROM today, i.e. if training period END = 30, the END period is 30 days ago. 30 days are a good number since most returns will have happened of sales that were created 30 days ago.
V_TRAINING_PERIOD_START NUMBER := 150; -- this is the start point of the training period in days FROM today.
V_DEFAULT  VARCHAR2(100) :='''unspecified'''; -- a default if brand IS NULL
V_TYPE_ID_SIMPLE  VARCHAR2(100) :='''simple''';
V_TYPE_ID_CONFIG  VARCHAR2(100) :='''configurable''';
V_DWH_SHIPMENT_STATE_1 VARCHAR2(100) :='''item_invalid''';
v_dwh_shipment_state_2 VARCHAR2(100) :='''item_canceled''';
V_DWH_SHIPMENT_STATE_3 VARCHAR2(100) :='''item_returned''';
V_DWH_SHIPMENT_STATE_4 VARCHAR2(100) :='''item_exchanged''';
V_DWH_SHIPMENT_STATE_5 VARCHAR2(100) :='''item_rejected''';
v_sql CLOB;

BEGIN

/* EXECUTE IMMEDIATE 'CREATE TABLE '|| V_VENTURE ||'_RTN_CNCL_FC AS */



	SELECT
		config_sku,
		brand,
		venture,
		sold_items_bfr_cancel_config,
		CASE
			WHEN sold_items_bfr_cancel_config >= '|| V_MINIMUM_SALES ||' THEN return_rate_config_lvl
			WHEN sold_items_bfr_cancel_config < '|| V_MINIMUM_SALES ||' AND sold_items_bfr_cancel_brand >= '|| V_MINIMUM_SALES ||' THEN return_rate_brand_lvl
			ELSE return_rate_total_lvl
		END AS return_rate_forecast,
		cancel_rate_total_lvl AS cancel_rate_forecast
	FROM
		(
			SELECT
				config_sku,
				config_level.brand,
				config_level.venture,
				config_level.sold_items_before_cancellation AS sold_items_bfr_cancel_config,
				brand_level.sold_items_before_cancellation AS sold_items_bfr_cancel_brand,
				return_rate_config_lvl,
				return_rate_brand_lvl,
				return_rate_total_lvl,
				cancel_rate_total_lvl
			FROM
				(
---------- calculation of historical return rates ON config level ------------------------------------------------
					SELECT
						'''|| V_VENTURE ||''' AS venture,
						config_sku,
						brand,
						sold_items_before_cancellation,
						canceled_items,
						returned_items,
						CASE
							WHEN sold_items_before_cancellation - canceled_items = 0 THEN 0
							ELSE round(returned_items/(sold_items_before_cancellation - canceled_items),2)
						END AS return_rate_config_lvl
					FROM
						(
							SELECT
								--'''|| V_VENTURE ||''' AS venture,
								sales_cancels.config_sku,
								brand,
								sold_items_before_cancellation,
								canceled_items,
								CASE
									WHEN returned_items IS NULL THEN 0
									ELSE returned_items
								END AS returned_items,
								CASE
									WHEN returned_items IS NULL THEN canceled_items
									ELSE returned_items + canceled_items
								END AS rtnd_and_cncl_items
							FROM
								(
									SELECT
										sales.config_sku,
										brand,
										sold_items_before_cancellation,
										CASE
											WHEN canceled_items IS NULL THEN 0
											ELSE canceled_items
										END AS canceled_items
									FROM
										(
											SELECT
												product.config_sku
												, nvl(brand,'|| V_DEFAULT ||') AS brand
												, count (id_sales_order_item) AS sold_items_before_cancellation
											FROM '|| V_VENTURE ||'.sales_order_item sales
											JOIN
												(
													SELECT
														DISTINCT a.sku, -- DISTINCT is necessary because there can be multiple entries per sku which would overestimate sales
														b.config_sku,
														a.brand
													FROM
														'|| V_VENTURE ||'.product a
													JOIN
														(
															SELECT
																DISTINCT sku AS config_sku,
																entity_id
															FROM
																'|| V_VENTURE ||'.product
															WHERE
																type_id = '|| V_TYPE_ID_CONFIG ||'
														) b
													ON a.parent_id = b.entity_id
													WHERE
														type_id = '|| V_TYPE_ID_SIMPLE ||'
												) product
												ON sales.sku = product.sku
											WHERE
												1=1
												AND trunc(sales.created_at) < (
																				SELECT
																					max(created_at) -'|| V_TRAINING_PERIOD_END ||'
																				FROM
																					'|| V_VENTURE ||'.sales_order_item
																				)
												AND trunc(sales.created_at) >= (
																				SELECT max(created_at) -'|| V_TRAINING_PERIOD_START ||'
																				FROM '|| V_VENTURE ||'.sales_order_item
																				)
												AND dwh_shipment_state NOT IN ('|| V_DWH_SHIPMENT_STATE_1 ||')
											GROUP BY
												product.config_sku
												,nvl(brand,'|| V_DEFAULT ||')
										) sales
										LEFT JOIN
											(
												SELECT
													product.config_sku
													--,brand
													, COUNT (id_sales_order_item) AS canceled_items
												FROM
													'|| V_VENTURE ||'.sales_order_item sales
												JOIN
													(
														SELECT
															DISTINCT a.sku,
															b.config_sku,
															a.brand
														FROM
															'|| V_VENTURE ||'.product a
														JOIN
															(
																SELECT
																	DISTINCT sku AS config_sku,
																	entity_id
																FROM
																	'|| V_VENTURE ||'.product
																WHERE
																	type_id = '|| V_TYPE_ID_CONFIG ||'
															) b
														ON a.parent_id = b.entity_id
														WHERE
															type_id = '|| V_TYPE_ID_SIMPLE ||'
													) product
													ON sales.sku = product.sku
												WHERE
													1=1
													AND trunc(created_at) < (
																				SELECT
																					max(created_at) -'|| V_TRAINING_PERIOD_END ||'
																				FROM
																					'|| V_VENTURE ||'.sales_order_item
																			)
													AND trunc(created_at) >= (
																				SELECT
																					max(created_at) -'|| V_TRAINING_PERIOD_START ||'
																				FROM
																					'|| V_VENTURE ||'.sales_order_item)
													AND dwh_shipment_state IN ('|| V_DWH_SHIPMENT_STATE_2 ||')
												GROUP BY
														product.config_sku
														--, brand
											) cancellations
											ON sales.config_sku = cancellations.config_sku
								) sales_cancels
								LEFT JOIN
									(
										SELECT
											product.config_sku
											--,brand
											, count (id_sales_order_item) AS returned_items
										FROM
											'|| V_VENTURE ||'.sales_order_item sales
										JOIN
											(
												SELECT
													DISTINCT a.sku,
													b.config_sku,
													a.brand
												FROM
													'|| V_VENTURE ||'.product a
												JOIN
													(
														SELECT
															DISTINCT sku AS config_sku,
															entity_id
														FROM
															'|| V_VENTURE ||'.product
														WHERE
															type_id = '|| V_TYPE_ID_CONFIG ||'
													) b
													ON a.parent_id = b.entity_id
												WHERE
													type_id = '|| V_TYPE_ID_SIMPLE ||'
											) product
											ON sales.sku = product.sku
										WHERE
											1=1
											AND trunc(created_at) < (
																		SELECT
																			max(created_at) -'|| V_TRAINING_PERIOD_END ||'
																		FROM '|| V_VENTURE ||'.sales_order_item
																	)
											AND trunc(created_at) >= (
																		SELECT
																			max(created_at) -'|| V_TRAINING_PERIOD_START ||'
																		FROM
																			'|| V_VENTURE ||'.sales_order_item
																	)
											AND dwh_shipment_state in ('|| V_DWH_SHIPMENT_STATE_3 ||', '|| V_DWH_SHIPMENT_STATE_4 ||','|| V_DWH_SHIPMENT_STATE_5 ||')
										GROUP BY
											product.config_sku
											--, brand
									) returns
									ON sales_cancels.config_sku = returns.config_sku
						)
				) config_level
			JOIN
				(
---------- calculation of historical return rates ON brand level ------------------------------------------------
					SELECT
						'''|| V_VENTURE ||''' AS venture,
						brand,
						sold_items_before_cancellation,
						CASE
							WHEN sold_items_before_cancellation - canceled_items = 0 THEN 0
							ELSE round(returned_items/(sold_items_before_cancellation - canceled_items),2)
						END AS return_rate_brand_lvl
					FROM
						(
							SELECT
								--'''|| V_VENTURE ||''' AS venture,
								sales_cancels.brand,
								sold_items_before_cancellation,
								canceled_items,
								CASE
									WHEN returned_items IS NULL THEN 0
									ELSE returned_items
								END AS returned_items,
								CASE
									WHEN returned_items IS NULL THEN canceled_items
									ELSE returned_items + canceled_items
								END AS rtnd_and_cncl_items
							FROM
								(
									SELECT
										sales.brand,
										sold_items_before_cancellation,
										CASE
											WHEN canceled_items IS NULL THEN 0
											ELSE canceled_items
										END AS canceled_items
									FROM
										(
											SELECT
												nvl(brand,'|| V_DEFAULT ||') AS brand
												, count (id_sales_order_item) AS sold_items_before_cancellation
											FROM
												'|| V_VENTURE ||'.sales_order_item sales
											JOIN
												(
													SELECT
														DISTINCT sku,
														brand
													FROM
														'|| V_VENTURE ||'.product
												) product
												ON sales.sku = product.sku
											WHERE
												1=1
												AND trunc(sales.created_at) < (
																				SELECT
																					max(created_at) -'|| V_TRAINING_PERIOD_END ||'
																				FROM
																					'|| V_VENTURE ||'.sales_order_item
																				)
												AND trunc(sales.created_at) >= (
																				SELECT
																					max(created_at) -'|| V_TRAINING_PERIOD_START ||'
																				FROM
																					'|| V_VENTURE ||'.sales_order_item)
												AND dwh_shipment_state NOT IN ('|| V_DWH_SHIPMENT_STATE_1 ||')
											GROUP BY
												nvl(brand,'|| V_DEFAULT ||')
										) sales
									LEFT JOIN
										(
											SELECT
												nvl(brand,'|| V_DEFAULT ||') AS brand
												, count (id_sales_order_item) AS canceled_items
											FROM '|| V_VENTURE ||'.sales_order_item sales
											JOIN
												(
													SELECT
														DISTINCT sku,
															brand
													FROM
														'|| V_VENTURE ||'.product
												) product
												ON sales.sku = product.sku
											WHERE
												1=1
												AND trunc(sales.created_at) < (
																				SELECT
																					max(created_at) -'|| V_TRAINING_PERIOD_END ||'
																				FROM
																					'|| V_VENTURE ||'.sales_order_item
																				)
												AND trunc(sales.created_at) >= (
																				SELECT
																					max(created_at) -'|| V_TRAINING_PERIOD_START ||'
																				FROM
																					'|| V_VENTURE ||'.sales_order_item
																				)
												AND dwh_shipment_state in ('|| V_DWH_SHIPMENT_STATE_2 ||')
												GROUP BY nvl(brand,'|| V_DEFAULT ||')
										) cancellations
										ON sales.brand = cancellations.brand
								) sales_cancels
								LEFT JOIN
									(
										SELECT
											nvl(brand,'|| V_DEFAULT ||') AS brand
											, count (id_sales_order_item) AS returned_items
										FROM
											'|| V_VENTURE ||'.sales_order_item sales
										JOIN
											(
												SELECT
													DISTINCT sku,
													brand
												FROM
													'|| V_VENTURE ||'.product
											) product
											ON sales.sku = product.sku
										WHERE
											1=1
											AND trunc(sales.created_at) < (
																			SELECT
																				max(created_at) -'|| V_TRAINING_PERIOD_END ||'
																			FROM
																				'|| V_VENTURE ||'.sales_order_item
																			)
											AND trunc(sales.created_at) >= (
																			SELECT
																				max(created_at) -'|| V_TRAINING_PERIOD_START ||'
																			FROM
																				'|| V_VENTURE ||'.sales_order_item
																			)
											AND dwh_shipment_state in ('|| V_DWH_SHIPMENT_STATE_3 ||', '|| V_DWH_SHIPMENT_STATE_4 ||','|| V_DWH_SHIPMENT_STATE_5 ||')
											GROUP BYf nvl(brand,'|| V_DEFAULT ||')
									) returns
									ON sales_cancels.brand = returns.brand
						)
				) brand_level
				ON config_level.brand = brand_level.brand
			JOIN
				(
---------- calculation of historical return AND cancelation rates ON total level ------------------------------------------------
					SELECT
						venture,
						sold_items_before_cancellation,
						CASE
							WHEN sold_items_before_cancellation = 0 THEN 0
							ELSE round(returned_items/(sold_items_before_cancellation - canceled_items),2)
						END AS return_rate_total_lvl,
						CASE
							WHEN sold_items_before_cancellation = 0 THEN 0
							ELSE round(canceled_items/sold_items_before_cancellation,2)
						END AS cancel_rate_total_lvl
					FROM
						(
							SELECT
								--'''|| V_VENTURE ||''' AS venture,
								sales_cancels.venture,
								sold_items_before_cancellation,
								canceled_items,
								CASE
									WHEN returned_items IS NULL THEN 0
									ELSE returned_items
								END AS returned_items,
								CASE
									WHEN returned_items IS NULL THEN canceled_items
									ELSE returned_items + canceled_items
								END AS rtnd_and_cncl_items
							FROM
								(
									SELECT
										sales.venture,
										sold_items_before_cancellation,
										CASE
											WHEN canceled_items IS NULL THEN 0
											ELSE canceled_items
										END AS canceled_items
									FROM
										(
											SELECT
												'''|| V_VENTURE ||''' AS venture
												, count (id_sales_order_item) AS sold_items_before_cancellation
											FROM
												'|| V_VENTURE ||'.sales_order_item sales
											WHERE
												1=1
												AND trunc(sales.created_at) < (
																				SELECT
																					max(created_at) -'|| V_TRAINING_PERIOD_END ||'
																				FROM
																					'|| V_VENTURE ||'.sales_order_item
																				)
												AND trunc(sales.created_at) >= (
																				SELECT
																					max(created_at) -'|| V_TRAINING_PERIOD_START ||'
																				FROM
																					'|| V_VENTURE ||'.sales_order_item
																				)
												AND dwh_shipment_state not in ('|| V_DWH_SHIPMENT_STATE_1 ||')
											GROUP BY '''|| V_VENTURE ||'''
										) sales
									LEFT JOIN
										(
											SELECT
												'''|| V_VENTURE ||'''  AS venture
												, count (id_sales_order_item) AS canceled_items
											FROM
												'|| V_VENTURE ||'.sales_order_item sales
											WHERE
												1=1
												AND trunc(sales.created_at) < (
																				SELECT
																					max(created_at) -'|| V_TRAINING_PERIOD_END ||'
																				FROM
																					'|| V_VENTURE ||'.sales_order_item
																				)
												AND trunc(sales.created_at) >= (
																				SELECT
																					max(created_at) -'|| V_TRAINING_PERIOD_START ||'
																				FROM
																					'|| V_VENTURE ||'.sales_order_item
																				)
												AND dwh_shipment_state in ('|| V_DWH_SHIPMENT_STATE_2 ||')
											GROUP BY '''|| V_VENTURE ||'''
										) cancellations
										ON sales.venture = cancellations.venture
								) sales_cancels
							LEFT JOIN
								(
									SELECT
										'''|| V_VENTURE ||'''  AS venture
										, count (id_sales_order_item) AS returned_items
									FROM
										'|| V_VENTURE ||'.sales_order_item sales
									JOIN
										'|| V_VENTURE ||'.product product
										ON sales.sku = product.sku
									WHERE
										1=1
										AND trunc(sales.created_at) < (
																		SELECT
																			max(created_at) -'|| V_TRAINING_PERIOD_END ||'
																		FROM
																			'|| V_VENTURE ||'.sales_order_item
																		)
										AND trunc(sales.created_at) >= (
																		SELECT
																			max(created_at) -'|| V_TRAINING_PERIOD_START ||'
																		FROM
																			'|| V_VENTURE ||'.sales_order_item
																		)
										AND dwh_shipment_state in ('|| V_DWH_SHIPMENT_STATE_3 ||', '|| V_DWH_SHIPMENT_STATE_4 ||','|| V_DWH_SHIPMENT_STATE_5 ||')
									GROUP BY '''|| V_VENTURE ||'''
								) returns
								ON sales_cancels.venture = returns.venture
						)
				) total_level
				ON config_level.venture = total_level.venture
		)
';


--dbms_output.put_line(v_sql);

/* ***************************************************************************************************************** */

END;
