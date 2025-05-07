# SQL - GA_SESSION  
In this project, I explore user behavior data by writing 8 SQL queries in Google BigQuery, based on the Google Analytics sample dataset — specifically focusing on the ga_sessions table.  
## 1. Introduce
Source: Google Analytics sample dataset  
Table: bigquery-public-data.google_analytics_sample.ga_sessions_*  
Mô tả
   <img width="421" alt="S1" src="https://github.com/user-attachments/assets/10493d42-e6c7-415d-916d-6c78b32c9593" />
   <img width="569" alt="s2" src="https://github.com/user-attachments/assets/199dc079-058a-4404-9eba-e7adebf21a79" />
3. Code
Query 01: calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)

SELECT
  SUM(totals.visits) AS visits,
  SUM(totals.pageviews) AS pageviews,
  SUM(totals.transactions) AS transactions,
  FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _TABLE_SUFFIX BETWEEN '0101' AND '0331'
GROUP BY month
ORDER BY month;
- Result
 <img width="379" alt="S3" src="https://github.com/user-attachments/assets/954623d5-9243-42d4-820b-02bbaea651f8" />
 
Query 02: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)

SELECT
  trafficSource.`source` AS sources
  ,SUM(totals.bounces) AS total_no_of_bounces
  ,SUM(totals.visits) AS total_visits
  ,ROUND((100* SUM(totals.bounces)/SUM(totals.visits)),3) AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _TABLE_SUFFIX BETWEEN '0701' AND '0731'
GROUP BY trafficSource.`source`
ORDER BY total_visits DESC;
- Result
<img width="406" alt="S4" src="https://github.com/user-attachments/assets/98f1f6f2-35e9-43b0-a9c0-1efcc9847903" />

Query 3: Revenue by traffic source by week, by month in June 2017 

WITH 
month_data as(
  SELECT
    "Month" as time_type,
    FORMAT_DATE("%Y%m", parse_date("%Y%m%d", date)) as month,
    trafficSource.source AS source,
    SUM(p.productRevenue)/1000000 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
    UNNEST(hits) hits,
    UNNEST(product) p
  WHERE p.productRevenue is not null
  GROUP BY 1,2,3
  ORDER BY revenue DESC
),

week_data as(
  SELECT
    "Week" as time_type,
    FORMAT_DATE("%Y%W", parse_date("%Y%m%d", date)) as week,
    trafficSource.source AS source,
    SUM(p.productRevenue)/1000000 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
  	 UNNEST(hits) hits,
   	UNNEST(product) p
  WHERE p.productRevenue is not null
  GROUP BY 1,2,3
  ORDER BY revenue DESC
)

SELECT *
FROM month_data
UNION ALL
SELECT * 
FROM week_data;
- Result
<img width="412" alt="S5" src="https://github.com/user-attachments/assets/b0c35b7f-26d8-4f98-8ff2-b32c5f42e259" />

Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. 

SELECT
  FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
  CASE  WHEN totals.transactions >= 1 AND product.productRevenue IS NOT NULL
        THEN 'purchase'
        WHEN totals.transactions IS NULL AND product.productRevenue IS NULL
        THEN 'non-purchase'
        ELSE 'unknown' END AS user_type,
  COUNT(DISTINCT fullVisitorId) AS unique_users,
  SUM(totals.pageviews) AS total_pageviews,
  SUM(totals.pageviews) / COUNT(DISTINCT fullVisitorId) AS avg_pageviews
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,  
UNNEST(hits) AS hits,
UNNEST(hits.product) AS product
WHERE PARSE_DATE('%Y%m%d', date) 
      BETWEEN DATE '2017-06-01' AND DATE '2017-07-31'
GROUP BY  month, user_type;

- Result
<img width="412" alt="S6" src="https://github.com/user-attachments/assets/6b648c62-5dbb-4d8b-89b8-21a3112d5c84" />

Query 05: Average number of transactions per user that made a purchase in July 2017 

SELECT
    FORMAT_DATE("%Y%m",parse_date("%Y%m%d",date)) as month,
    	SUM(totals.transactions)/count(distinct fullvisitorid) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST (hits) hits,
    UNNEST(product) product
WHERE  totals.transactions>=1
AND product.productRevenue is not null
GROUP BY month;
- Result
<img width="365" alt="S7" src="https://github.com/user-attachments/assets/10e7d242-57a5-4107-a593-eed9f8bb130c" />

Query 06: Average amount of money spent per session. Only include purchaser data in July 2017 

SELECT
   FORMAT_DATE('%Y%m', PARSE_DATE ("%Y%m%d", DATE)) AS month,
  SUM(totals.visits) AS total_visit,
  SUM(productRevenue) AS total_revenue,
  (SUM(productRevenue) / SUM(totals.visits))/1000000 AS avg_total_revenue_per_user_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
UNNEST(hits) AS hits,
UNNEST(hits.product) AS product
WHERE 
_TABLE_SUFFIX BETWEEN '0701' AND '0731' 
AND totals.transactions IS NOT NULL 
AND product.productRevenue IS NOT NULL
GROUP BY  month;
- Result
<img width="416" alt="S8" src="https://github.com/user-attachments/assets/558ac3ad-3b0d-4431-9a3d-9a99777a8a52" />

Query 07: Other products purchased by customers who purchased the product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered. 

WITH u AS (
  SELECT
   distinct(fullVisitorId)
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) AS hits,
  UNNEST(hits.product) AS product
  WHERE productRevenue IS NOT NULL
      AND v2ProductName= "YouTube Men's Vintage Henley"
      AND _TABLE_SUFFIX BETWEEN '0701' AND '0731'
)
SELECT
  v2ProductName as other_purchased_products,
  SUM(productQuantity) AS total_quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` AS m,
UNNEST(hits) AS hits,
UNNEST(hits.product) AS product
INNER JOIN u
ON m.fullVisitorId = u.fullVisitorId
WHERE productRevenue IS NOT NULL
      AND v2ProductName != "YouTube Men's Vintage Henley"
      AND _TABLE_SUFFIX BETWEEN '0701' AND '0731'
GROUP BY v2ProductName
ORDER BY total_quantity DESC;

- Result
<img width="351" alt="S9" src="https://github.com/user-attachments/assets/b3cb38ad-8111-480e-a662-34716068117d" />

  Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase.

WITH product_data as(
SELECT
    FORMAT_DATE('%Y%m', parse_date('%Y%m%d',date)) as month,
    COUNT(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    COUNT(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' and product.productRevenue is not null THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
 UNNEST(hits) as hits,
UNNEST (hits.product) as product
WHERE _table_suffix between '20170101' and '20170331'
AND eCommerceAction.action_type in ('2','3','6')
GROUP BY month
ORDER BY month
)

SELECT
    *,
    ROUND(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    ROUND(num_purchase/num_product_view * 100, 2) as purchase_rate
FROM product_data;
- Result
  <img width="404" alt="S10" src="https://github.com/user-attachments/assets/7489778f-afe7-4bf6-8bd3-ae3cd0357b21" />
