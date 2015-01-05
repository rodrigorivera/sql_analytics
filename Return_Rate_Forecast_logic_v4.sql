
/* Return Rate Forecast per Product SKU*/
SELECT 
	Product_SKU,
	targetgroup,
	description,
/* If SKU has been sold at least 8 times take return rate of SKU. If not take Return Rate of hierarchy level above. If there are
less than 8 sales on this hierarchy level before return the take return rate of highest level*/
	CASE 
		WHEN sold_articles_bef_return >= 8 THEN ROUND(return_rate_product,2)
		WHEN sold_articles_bef_return < 8 AND sold_articles_bef_return_tgt >= 8 THEN ROUND(return_rate_targetgroup,2)
else round(return_rate_description,2)
end as return_rate_forecast
--sold_articles_bef_return,
--sold_articles,
--sold_articles_bef_return_tgt,
--sold_articles_tgt,
--return_rate_product,
--return_rate_targetgroup,
--return_rate_description
from
(
----------------------------------------------------
/* Calculate historical return rates per SKU*/
select 
agg_product.Product_SKU,
--agg_product."SUBTYPE",
--agg_product."TYPE",
agg_product.targetgroup,
agg_product.description,
agg_product.sold_articles_bef_return,
agg_product.sold_articles,
agg_targetgroup.sold_articles_bef_return as sold_articles_bef_return_tgt,
agg_targetgroup.sold_articles as sold_articles_tgt,
case when agg_product.sold_articles_bef_return = 0 then 0
else (agg_product.sold_articles_bef_return - agg_product.sold_articles) / agg_product.sold_articles_bef_return
end as return_rate_product,
agg_targetgroup.return_rate_targetgroup,
agg_description.return_rate_description
from
(
/* Join targetgroup to sales per Product SKU*/
select 
Product_SKU,
targetgroup,
description,
sold_articles_bef_return,
sold_articles
from
(
/* Get Sales before and after returns per Product SKU*/
select
p.sku as Product_SKU,
sum(s.sold_articles_bef_return) as sold_articles_bef_return,
sum(s.sold_articles) as sold_articles
from fct_sales s
join dim_dates d on s.date_id = d.date_id
join dim_articles a on s.article_id = a.article_id
join dim_products p on a.product_id = p.product_id
/* Only look at time period with length 48 days which is 30 days ago. The 30 days are chosen based on the assumption
that most returns will happen within 30 days after the order creation date. This is a Zalando assumption and should
be adjusted based on the local requirements. The 48 days have been chosen semi-randomly. It should reflect a time period
which is long enough to be representative and not too long to be too heavily affected by seasonalities and external effects*/
where d.date_id < (select max(s.date_id) -30 from fct_sales s)
and d.date_id >= (select max(s.date_id) -78 from fct_sales s)
group by 
/* Group by must only be on one granularity level since each Product can historically be in multiple categories, thus creating
multiple datasets*/
p.sku 
) a
join
dim_products p on a.product_sku = p.sku
/* Improvement in v3: set max date of the observation period between valid_from and valid_to of the Product. Thus we join 
the categories to the SKU that were valid at the time of the end of theobservation period and we avoid that the SKU can be 
in multiple categories over time which would generate multiple rows */
where 
(
select 
to_date(iso_date, 'YYYY.MM.DD')
from
(
select max(s.date_id) -30 as max_date_id
from fct_sales s
) a
join
dim_dates d on a.max_date_id = d.date_id
) between p.valid_from and p.valid_to  
)
agg_product
/* We want to use return rates on Product SKU level if at least 8 items have been sold. If less items have been sold we are taking 
the return rate from the hierarchy above - in this case targetgroup. If in this hierarchy level we don't have 8 items as well
we move up one more level until we have reached the highest granularity level 
HERE WE ARE JOINING TARGETGROUP LEVEL*/
-----------------------------------------------------
join 
(
select 
targetgroup,
sold_articles_bef_return,
sold_articles,
case when sold_articles_bef_return = 0 then 0
else (sold_articles_bef_return - sold_articles) / sold_articles_bef_return
end as return_rate_targetgroup
from
(
select
p.targetgroup,
sum(s.sold_articles_bef_return) as sold_articles_bef_return,
sum(s.sold_articles) as sold_articles
from fct_sales s
join dim_dates d on s.date_id = d.date_id
join dim_articles a on s.article_id = a.article_id
join dim_products p on a.product_id = p.product_id
/* Only look at time period with length 48 days which is 30 days ago. The 30 days are chosen based on the assumption
that most returns will happen within 30 days after the order creation date. This is a Zalando assumption and should
be adjusted based on the local requirements. The 48 days have been chosen semi-randomly. It should reflect a time period
which is long enough to be representative and not too long to be too heavily affected by seasonalities and external effects*/
where d.date_id < (select max(s.date_id) -30 from fct_sales s)
and d.date_id >= (select max(s.date_id) -78 from fct_sales s)
group by 
p.targetgroup
)
) agg_targetgroup on agg_product.targetgroup = agg_targetgroup.targetgroup
/* We want to use return rates on Product SKU level if at least 8 items have been sold. If less items have been sold we are taking 
the return rate from the hierarchy above - in this case targetgroup. If in this hierarchy level we don't have 8 items as well
we move up one more level until we have reached the highest granularity level 
HERE WE ARE JOINING DESCRIPTION LEVEL*/
-----------------------------------------------------
join
(
select 
description,
sold_articles_bef_return,
sold_articles,
case when sold_articles_bef_return = 0 then 0
else (sold_articles_bef_return - sold_articles) / sold_articles_bef_return
end as return_rate_description
from
(
select
p.description,
sum(s.sold_articles_bef_return) as sold_articles_bef_return,
sum(s.sold_articles) as sold_articles
from fct_sales s
join dim_dates d on s.date_id = d.date_id
join dim_articles a on s.article_id = a.article_id
join dim_products p on a.product_id = p.product_id
/* Only look at time period with length 48 days which is 30 days ago. The 30 days are chosen based on the assumption
that most returns will happen within 30 days after the order creation date. This is a Zalando assumption and should
be adjusted based on the local requirements. The 48 days have been chosen semi-randomly. It should reflect a time period
which is long enough to be representative and not too long to be too heavily affected by seasonalities and external effects*/
where d.date_id < (select max(s.date_id) -30 from fct_sales s)
and d.date_id >= (select max(s.date_id) -78 from fct_sales s)
group by 
p.description
)
) agg_description on agg_product.description = agg_description.description
)
;

