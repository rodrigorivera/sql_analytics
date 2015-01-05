/* Replenishment Report based on Data Layer 
Still to do:
- join config SKU, brand, buyer, categories -> entity id, parent_id needs to be added in the inner select, then join based on the full select
- build aggregate reports on brand, config sku, categories
- include cost based KPIs
ATTENTION: check inventory type filter! Possibly needs to be adjusted per venture!
*/

--*****************************************************************************************************************


define observation_period = 10; -- this is the number of days that we calibrate the sales rate on
define missing_activated_at_factor = 14; -- if activated at is null this value will be added to created at to approximate activated at. SHould be the average time between created at and activated at
define observation_point = 1; -- this is the stock date we are observing





select sales_rate.sku
, round(sold_items_before_return/visibility_period,2) as sales_rate
, amount_on_stock
,sold_items_before_return
,visibility_period
, case when sold_items_before_return/visibility_period = 0 then 999 -- when sales rate is 0 then it takes an infinite time to deplete the stock
  else round(amount_on_stock/(sold_items_before_return/visibility_period),1) 
  end as inventory_days
, round(sold_items_before_return/amount_on_stock,3) as sell_through_rate_item_based
from
(
select 
visibility_sku.sku
, visibility_period
, case when sold_items_before_return is null then 0-- when sold_items_before return is null, then the SKU has not been sold in the observation period
  else sold_items_before_return
  end as sold_items_before_return
from
(
---------------------------------------------------------------------------------------------------
/* Step 1: Approximate visibility period per SKU by looking at the following things:
- how long has the SKU been activated? (activated_at)?
- within this period: how many days has the SKU been out of stock?*/
select days_active_sku.SKU,
case when days_without_stock is null then &observation_period -- when days without stock is null, then the SKU is on stock in the full observation period
else days_active_in_obs_period - days_without_stock 
end as visibility_period
from
(
/* Determine the lenght of the period that a simple SKU has been active within the observation period*/
select sku, 
case when round(sysdate - activated_at,0) > &observation_period then &observation_period -- days active is larger than observation period
when activated_at is null and round(sysdate - created_at + &missing_activated_at_factor,0) <= 21 then round(sysdate - created_at + &missing_activated_at_factor,0) -- when activated_at is null we will approximate activated at by taking created at and adding the average days for a product to switch from created at to activated at
when activated_at is null and round(sysdate - created_at + &missing_activated_at_factor,0) > 21 then &observation_period
else round(sysdate - activated_at,0)
end as days_active_in_obs_period, 
activated_at, 
created_at
from dg_mag_ctlg_prdct_shoes
where dg_end_date > sysdate -- needs to be eliminated for miniDWH
and status =1 -- only visible SKUs -> for Inventory Risk analysis this line needs to be removed since we want to analyse risk for all SKUs on stock
and inventory_type = 'Own Warehouse' -- needs to be adjusted for other ventures!! Only own warehouse goods are being analysed
and type_id = 'simple'
) days_active_sku
left join
(
/* Determine the lenght of the period that a simple SKU has been out of stock and thus was not visible*/
select sku,count(stock_date) as days_without_stock from dg_bob_stock_export
where round(sysdate - stock_date,0) <= &observation_period
and dg_end_date > sysdate -- needs to be eliminated for miniDWH
and quantity = 0
group by sku

) days_stock_sku
on days_active_sku.sku = days_stock_sku.sku
) visibility_sku
---------------------------------------------------------------------------------------------------
/* Step 2: Join of sales_before_return for each SKU to determine the sales rate
as sales_before_return / visibility period*/
left join
(
select sku 
, count (id_sales_order_item) as sold_items_before_return
from dg_bob_sales_order_item
where dg_end_date > sysdate
and round(sysdate - created_at,0) <= &observation_period -- only sales within observation period
and dwh_shipment_state not in ('item_invalid','item_canceled')
group by sku
) sales_sku
on visibility_sku.sku = sales_sku.sku
) sales_rate
---------------------------------------------------------------------------------------------------
/* Step 3: Join of stock amount at the observation day*/
left join
(
select sku, 
stock_date, 
sum (quantity) as amount_on_stock
from dg_bob_stock_export
where dg_end_date > sysdate
and round(sysdate - stock_date,0) = 1 -- needs to be transformed into a variable &observation_point. For some reason it did not work while it worked with &observation_period
group by sku, stock_date
) stock on sales_rate.sku = stock.sku
;

--*****************************************************************************************************************

