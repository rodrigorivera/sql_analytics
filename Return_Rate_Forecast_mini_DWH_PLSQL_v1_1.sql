/* RETURN AND CANCELLATION RATE FORECAST

Description:
Ventures face the problem that cancellation rates and return rates for the recent period are unkown. This analysis
predicts returns and cancellations on a config SKU level based on historical return and cancellation behavior.

Version: v1.1, 2012-October-29

Version history:
- v1.0 initial version
- v1.1 initial version was on simple SKU granularity. Adjusted to config granularity. Also added "distinct" to
product catalogue to make sure that sales are not counted multiple times when there is more than one entry per SKU
in the catalogue

Author: Rodrigo Rivera

Open Issues:
- when BOB provides unique categories per SKU we can also use categories for return rate proxies (v2.0)

*/

--*****************************************************************************************************************


SET SERVEROUTPUT ON

DECLARE

V_MINIMUM_SALES NUMBER := 30; -- when there is not a significant number on sales on each level, i.e. sales are < than this number, the return rate of a higher level is used. Example: config SKU-4711 of brand Samsung has 15 sales, but Samsung has 80 sales. In this case the return rate is taken from Samsung. Zalando uses 8, but analysis at Jabong has shown that at least 30 items are necessary that return rates are comparable over different periods.
V_VENTURE VARCHAR2(100) := 'namshi_ae'; -- this is the schema name of the venture
V_TRAINING_PERIOD_END NUMBER := 30; -- this is the end point of the training period in days from today, i.e. if training period end = 30, the end period is 30 days ago. 30 days are a good number since most returns will have happened of sales that were created 30 days ago.
V_TRAINING_PERIOD_START NUMBER := 150; -- this is the start point of the training period in days from today.
V_DEFAULT  VARCHAR2(100) :='''unspecified'''; -- a default if brand is null
V_TYPE_ID_SIMPLE  VARCHAR2(100) :='''simple''';
V_TYPE_ID_CONFIG  VARCHAR2(100) :='''configurable''';
V_DWH_SHIPMENT_STATE_1 VARCHAR2(100) :='''item_invalid''';
v_dwh_shipment_state_2 VARCHAR2(100) :='''item_canceled''';
V_DWH_SHIPMENT_STATE_3 VARCHAR2(100) :='''item_returned''';
V_DWH_SHIPMENT_STATE_4 VARCHAR2(100) :='''item_exchanged''';
V_DWH_SHIPMENT_STATE_5 VARCHAR2(100) :='''item_rejected''';
v_sql CLOB;

BEGIN

EXECUTE IMMEDIATE 'CREATE TABLE '|| V_VENTURE ||'_RTN_CNCL_FC AS



select
config_sku,
brand,
venture,
sold_items_bfr_cancel_config,
case
when sold_items_bfr_cancel_config >= '|| V_MINIMUM_SALES ||' then return_rate_config_lvl
when sold_items_bfr_cancel_config < '|| V_MINIMUM_SALES ||' and sold_items_bfr_cancel_brand >= '|| V_MINIMUM_SALES ||' then return_rate_brand_lvl
else return_rate_total_lvl
end as return_rate_forecast,
cancel_rate_total_lvl as cancel_rate_forecast
from
(
select
config_sku,
config_level.brand,
config_level.venture,
config_level.sold_items_before_cancellation as sold_items_bfr_cancel_config,
brand_level.sold_items_before_cancellation as sold_items_bfr_cancel_brand,
return_rate_config_lvl,
return_rate_brand_lvl,
return_rate_total_lvl,
cancel_rate_total_lvl
from
(
---------- calculation of historical return rates on config level ------------------------------------------------
select
'''|| V_VENTURE ||''' as venture,
config_sku,
brand,
sold_items_before_cancellation,
canceled_items,
returned_items,
case when sold_items_before_cancellation - canceled_items = 0 then 0
else round(returned_items/(sold_items_before_cancellation - canceled_items),2)
end as return_rate_config_lvl
from
(
select
--'''|| V_VENTURE ||''' as venture,
sales_cancels.config_sku,
brand,
sold_items_before_cancellation,
canceled_items,
case when returned_items is null then 0
else returned_items
end as returned_items,
case when returned_items is null then canceled_items
else returned_items + canceled_items
end as rtnd_and_cncl_items
from
(
select
sales.config_sku,
brand,
sold_items_before_cancellation,
case when canceled_items is null then 0
else canceled_items
end as canceled_items
from
(
select
product.config_sku
/*  NVL() is a function to replace a value if null is encountered */
, nvl(brand,'|| V_DEFAULT ||') as brand
, count (id_sales_order_item) as sold_items_before_cancellation
from '|| V_VENTURE ||'.sales_order_item sales
join
(
select
distinct a.sku, -- distinct is necessary because there can be multiple entries per sku which would overestimate sales
b.config_sku,
a.brand
from '|| V_VENTURE ||'.product a
join
(
select
distinct sku as config_sku,
entity_id
from '|| V_VENTURE ||'.product
where type_id = '|| V_TYPE_ID_CONFIG ||'
) b
on a.parent_id = b.entity_id
where type_id = '|| V_TYPE_ID_SIMPLE ||'
) product
on sales.sku = product.sku
where 1=1
and trunc(sales.created_at) < (select max(created_at) -'|| V_TRAINING_PERIOD_END ||' from '|| V_VENTURE ||'.sales_order_item)
and trunc(sales.created_at) >= (select max(created_at) -'|| V_TRAINING_PERIOD_START ||' from '|| V_VENTURE ||'.sales_order_item)
and dwh_shipment_state not in ('|| V_DWH_SHIPMENT_STATE_1 ||')
group by product.config_sku
,nvl(brand,'|| V_DEFAULT ||')
) sales
left join
(
select product.config_sku
--,brand
, count (id_sales_order_item) as canceled_items
from '|| V_VENTURE ||'.sales_order_item sales
join (
select
distinct a.sku,
b.config_sku,
a.brand
from '|| V_VENTURE ||'.product a
join
(
select
distinct sku as config_sku,
entity_id
from '|| V_VENTURE ||'.product
where type_id = '|| V_TYPE_ID_CONFIG ||'
) b
on a.parent_id = b.entity_id
where type_id = '|| V_TYPE_ID_SIMPLE ||'
) product on sales.sku = product.sku
where 1=1
and trunc(created_at) < (select max(created_at) -'|| V_TRAINING_PERIOD_END ||' from '|| V_VENTURE ||'.sales_order_item)
and trunc(created_at) >= (select max(created_at) -'|| V_TRAINING_PERIOD_START ||' from '|| V_VENTURE ||'.sales_order_item)
and dwh_shipment_state in ('|| V_DWH_SHIPMENT_STATE_2 ||')
group by product.config_sku
--, brand
) cancellations
on sales.config_sku = cancellations.config_sku
) sales_cancels
left join
(
select product.config_sku
--,brand
, count (id_sales_order_item) as returned_items
from '|| V_VENTURE ||'.sales_order_item sales
join (
select
distinct a.sku,
b.config_sku,
a.brand
from '|| V_VENTURE ||'.product a
join
(
select
distinct sku as config_sku,
entity_id
from '|| V_VENTURE ||'.product
where type_id = '|| V_TYPE_ID_CONFIG ||'
) b
on a.parent_id = b.entity_id
where type_id = '|| V_TYPE_ID_SIMPLE ||'
) product on sales.sku = product.sku
where 1=1
and trunc(created_at) < (select max(created_at) -'|| V_TRAINING_PERIOD_END ||' from '|| V_VENTURE ||'.sales_order_item)
and trunc(created_at) >= (select max(created_at) -'|| V_TRAINING_PERIOD_START ||' from '|| V_VENTURE ||'.sales_order_item)
and dwh_shipment_state in ('|| V_DWH_SHIPMENT_STATE_3 ||', '|| V_DWH_SHIPMENT_STATE_4 ||','|| V_DWH_SHIPMENT_STATE_5 ||')
group by product.config_sku
--, brand
) returns
on sales_cancels.config_sku = returns.config_sku
)
) config_level
join
(
---------- calculation of historical return rates on brand level ------------------------------------------------
select
'''|| V_VENTURE ||''' as venture,
brand,
sold_items_before_cancellation,
case when sold_items_before_cancellation - canceled_items = 0 then 0
else round(returned_items/(sold_items_before_cancellation - canceled_items),2)
end as return_rate_brand_lvl
from
(
select
--'''|| V_VENTURE ||''' as venture,
sales_cancels.brand,
sold_items_before_cancellation,
canceled_items,
case when returned_items is null then 0
else returned_items
end as returned_items,
case when returned_items is null then canceled_items
else returned_items + canceled_items
end as rtnd_and_cncl_items
from
(
select
sales.brand,
sold_items_before_cancellation,
case when canceled_items is null then 0
else canceled_items
end as canceled_items
from
(
select
nvl(brand,'|| V_DEFAULT ||') as brand
, count (id_sales_order_item) as sold_items_before_cancellation
from '|| V_VENTURE ||'.sales_order_item sales
join (
select
distinct sku,
brand
from '|| V_VENTURE ||'.product
) product on sales.sku = product.sku
where 1=1
and trunc(sales.created_at) < (select max(created_at) -'|| V_TRAINING_PERIOD_END ||' from '|| V_VENTURE ||'.sales_order_item)
and trunc(sales.created_at) >= (select max(created_at) -'|| V_TRAINING_PERIOD_START ||' from '|| V_VENTURE ||'.sales_order_item)
and dwh_shipment_state not in ('|| V_DWH_SHIPMENT_STATE_1 ||')
group by nvl(brand,'|| V_DEFAULT ||')
) sales
left join
(
select
nvl(brand,'|| V_DEFAULT ||') as brand
, count (id_sales_order_item) as canceled_items
from '|| V_VENTURE ||'.sales_order_item sales
join (
select
distinct sku,
brand
from '|| V_VENTURE ||'.product
) product on sales.sku = product.sku
where 1=1
and trunc(sales.created_at) < (select max(created_at) -'|| V_TRAINING_PERIOD_END ||' from '|| V_VENTURE ||'.sales_order_item)
and trunc(sales.created_at) >= (select max(created_at) -'|| V_TRAINING_PERIOD_START ||' from '|| V_VENTURE ||'.sales_order_item)
and dwh_shipment_state in ('|| V_DWH_SHIPMENT_STATE_2 ||')
group by nvl(brand,'|| V_DEFAULT ||')
) cancellations
on sales.brand = cancellations.brand
) sales_cancels
left join
(
select
nvl(brand,'|| V_DEFAULT ||') as brand
, count (id_sales_order_item) as returned_items
from '|| V_VENTURE ||'.sales_order_item sales
join (
select
distinct sku,
brand
from '|| V_VENTURE ||'.product
) product on sales.sku = product.sku
where 1=1
and trunc(sales.created_at) < (select max(created_at) -'|| V_TRAINING_PERIOD_END ||' from '|| V_VENTURE ||'.sales_order_item)
and trunc(sales.created_at) >= (select max(created_at) -'|| V_TRAINING_PERIOD_START ||' from '|| V_VENTURE ||'.sales_order_item)
and dwh_shipment_state in ('|| V_DWH_SHIPMENT_STATE_3 ||', '|| V_DWH_SHIPMENT_STATE_4 ||','|| V_DWH_SHIPMENT_STATE_5 ||')
group by nvl(brand,'|| V_DEFAULT ||')
) returns
on sales_cancels.brand = returns.brand
)
) brand_level
on config_level.brand = brand_level.brand
join
(
---------- calculation of historical return and cancelation rates on total level ------------------------------------------------


select
venture,
sold_items_before_cancellation,
case when sold_items_before_cancellation = 0 then 0
else round(returned_items/(sold_items_before_cancellation - canceled_items),2)
end as return_rate_total_lvl,
case when sold_items_before_cancellation = 0 then 0
else round(canceled_items/sold_items_before_cancellation,2)
end as cancel_rate_total_lvl
from
(
select
--'''|| V_VENTURE ||''' as venture,
sales_cancels.venture,
sold_items_before_cancellation,
canceled_items,
case when returned_items is null then 0
else returned_items
end as returned_items,
case when returned_items is null then canceled_items
else returned_items + canceled_items
end as rtnd_and_cncl_items
from
(
select
sales.venture,
sold_items_before_cancellation,
case when canceled_items is null then 0
else canceled_items
end as canceled_items
from
(
select
'''|| V_VENTURE ||''' as venture
, count (id_sales_order_item) as sold_items_before_cancellation
from '|| V_VENTURE ||'.sales_order_item sales
where 1=1
and trunc(sales.created_at) < (select max(created_at) -'|| V_TRAINING_PERIOD_END ||' from '|| V_VENTURE ||'.sales_order_item)
and trunc(sales.created_at) >= (select max(created_at) -'|| V_TRAINING_PERIOD_START ||' from '|| V_VENTURE ||'.sales_order_item)
and dwh_shipment_state not in ('|| V_DWH_SHIPMENT_STATE_1 ||')
group by '''|| V_VENTURE ||'''
) sales
left join
(
select
'''|| V_VENTURE ||'''  as venture
, count (id_sales_order_item) as canceled_items
from '|| V_VENTURE ||'.sales_order_item sales
where 1=1
and trunc(sales.created_at) < (select max(created_at) -'|| V_TRAINING_PERIOD_END ||' from '|| V_VENTURE ||'.sales_order_item)
and trunc(sales.created_at) >= (select max(created_at) -'|| V_TRAINING_PERIOD_START ||' from '|| V_VENTURE ||'.sales_order_item)
and dwh_shipment_state in ('|| V_DWH_SHIPMENT_STATE_2 ||')
group by '''|| V_VENTURE ||'''
) cancellations
on sales.venture = cancellations.venture
) sales_cancels
left join
(
select
'''|| V_VENTURE ||'''  as venture
, count (id_sales_order_item) as returned_items
from '|| V_VENTURE ||'.sales_order_item sales
join '|| V_VENTURE ||'.product product on sales.sku = product.sku
where 1=1
and trunc(sales.created_at) < (select max(created_at) -'|| V_TRAINING_PERIOD_END ||' from '|| V_VENTURE ||'.sales_order_item)
and trunc(sales.created_at) >= (select max(created_at) -'|| V_TRAINING_PERIOD_START ||' from '|| V_VENTURE ||'.sales_order_item)
and dwh_shipment_state in ('|| V_DWH_SHIPMENT_STATE_3 ||', '|| V_DWH_SHIPMENT_STATE_4 ||','|| V_DWH_SHIPMENT_STATE_5 ||')
group by '''|| V_VENTURE ||'''
) returns
on sales_cancels.venture = returns.venture
)
) total_level
on config_level.venture = total_level.venture
)
';


--dbms_output.put_line(v_sql);

--*****************************************************************************************************************

END;
