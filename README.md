# SQL - GA_SESSION  
In this project, I explore user behavior data by writing 8 SQL queries in Google BigQuery, based on the Google Analytics sample dataset — specifically focusing on the ga_sessions table.  
## 1. Introduce
Source: Google Analytics sample dataset  
Table: bigquery-public-data.google_analytics_sample.ga_sessions_*  
Description<br>  
<img width="887" alt="Description" src="https://github.com/user-attachments/assets/97d0c3fa-2cad-41ce-aa55-c4e2ba3a38b1" /><br>

## 2. Code  
**Query 01: calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)** <br>
SELECT<br>
  SUM(totals.visits) AS visits,<br>
  SUM(totals.pageviews) AS pageviews,<br>
  SUM(totals.transactions) AS transactions,<br>
  FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month<br>
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*<br>`
WHERE _TABLE_SUFFIX BETWEEN '0101' AND '0331'<br>
GROUP BY month<br>
ORDER BY month;<br>
- Result<br>
 <img width="379" alt="S3" src="https://github.com/user-attachments/assets/954623d5-9243-42d4-820b-02bbaea651f8" />
 
**Query 02: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)** <br>
SELECT<br>
  trafficSource.`source` AS sources,<br>
  SUM(totals.bounces) AS total_no_of_bounces,<br>
  SUM(totals.visits) AS total_visits,<br>
  ROUND((100* SUM(totals.bounces)/SUM(totals.visits)),3) AS bounce_rate<br>
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`<br>
WHERE _TABLE_SUFFIX BETWEEN '0701' AND '0731'<br>
GROUP BY trafficSource.`source`<br>
ORDER BY total_visits DESC;<br>
- Result<br>
<img width="406" alt="S4" src="https://github.com/user-attachments/assets/98f1f6f2-35e9-43b0-a9c0-1efcc9847903" /><br>

**Query 3: Revenue by traffic source by week, by month in June 2017** <br>
WITH month_data AS ( <br>
  SELECT<br>
    "Month" AS time_type,<br>
    FORMAT_DATE("%Y%m", parse_date("%Y%m%d", date)) AS month,<br>
    trafficSource.source AS source,<br>
    SUM(p.productRevenue)/1000000 AS revenue<br>
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,<br>
    UNNEST(hits) hits,<br>
    UNNEST(product) p <br>
  WHERE p.productRevenue is not null <br>
  GROUP BY 1,2,3 <br>
  ORDER BY revenue DESC <br>
),<br>

week_data AS( <br>
  SELECT <br>
    "Week" AS time_type,<br>
    FORMAT_DATE("%Y%W", parse_date("%Y%m%d", date)) AS week,<br>
    trafficSource.source AS source,<br>
    SUM(p.productRevenue)/1000000 AS revenue <br>
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,<br>
  	 UNNEST(hits) hits,<br>
   	UNNEST(product) p <br>
  WHERE p.productRevenue is not null <br>
  GROUP BY 1,2,3 <br>
  ORDER BY revenue DESC <br>
) <br>

SELECT * <br>
FROM month_data <br>
UNION ALL <br>
SELECT * <br>
FROM week_data; <br>
- Result <br>
<img width="412" alt="S5" src="https://github.com/user-attachments/assets/b0c35b7f-26d8-4f98-8ff2-b32c5f42e259" />

**Query 04: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017** <br> 
SELECT<br>
  FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,<br>
  CASE  WHEN totals.transactions >= 1 AND product.productRevenue IS NOT NULL<br>
        THEN 'purchase'<br>
        WHEN totals.transactions IS NULL AND product.productRevenue IS NULL<br>
        THEN 'non-purchase'<br>
        ELSE 'unknown' END AS user_type,<br>
  COUNT(DISTINCT fullVisitorId) AS unique_users,<br>
  SUM(totals.pageviews) AS total_pageviews,<br>
  SUM(totals.pageviews) / COUNT(DISTINCT fullVisitorId) AS avg_pageviews<br>
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,  <br>
UNNEST(hits) AS hits,<br>
UNNEST(hits.product) AS product<br>
WHERE PARSE_DATE('%Y%m%d', date) <br>
      BETWEEN DATE '2017-06-01' AND DATE '2017-07-31'<br>
GROUP BY  month, user_type;<br>

- Result<br>
<img width="412" alt="S6" src="https://github.com/user-attachments/assets/6b648c62-5dbb-4d8b-89b8-21a3112d5c84" /> <br>

**Query 05: Average number of transactions per user that made a purchase in July 2017** <br>

SELECT<br>
    FORMAT_DATE("%Y%m",parse_date("%Y%m%d",date)) AS month,<br>
    	SUM(totals.transactions)/count(distinct fullvisitorid) AS Avg_total_transactions_per_user<br>
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,<br>
    UNNEST (hits) hits,<br>
    UNNEST(product) product<br>
WHERE  totals.transactions>=1<br>
AND product.productRevenue is not null<br>
GROUP BY month;<br>
- Result<br>
<img width="365" alt="S7" src="https://github.com/user-attachments/assets/10e7d242-57a5-4107-a593-eed9f8bb130c" /> <br>

**Query 06: Average amount of money spent per session. Only include purchaser data in July 2017** <br>
SELECT<br>
   FORMAT_DATE('%Y%m', PARSE_DATE ("%Y%m%d", DATE)) AS month,<br>
  SUM(totals.visits) AS total_visit,<br>
  SUM(productRevenue) AS total_revenue,<br>
  (SUM(productRevenue) / SUM(totals.visits))/1000000 AS avg_total_revenue_per_user_visit<br>
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,<br>
UNNEST(hits) AS hits,<br>
UNNEST(hits.product) AS product<br>
WHERE _TABLE_SUFFIX BETWEEN '0701' AND '0731' <br>
  AND totals.transactions IS NOT NULL <br>
  AND product.productRevenue IS NOT NULL<br>
GROUP BY  month;<br>
- Result<br>
<img width="416" alt="S8" src="https://github.com/user-attachments/assets/558ac3ad-3b0d-4431-9a3d-9a99777a8a52" /> <br>

**Query 07: Other products purchased by customers who purchased the product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered** <br>
WITH u AS ( <br>
  SELECT distinct(fullVisitorId) <br>
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`, <br>
  UNNEST(hits) AS hits, <br>
  UNNEST(hits.product) AS product <br>
  WHERE productRevenue IS NOT NULL <br>
      AND v2ProductName= "YouTube Men's Vintage Henley" <br>
      AND _TABLE_SUFFIX BETWEEN '0701' AND '0731' <br>
)<br>
SELECT<br>
  v2ProductName AS other_purchased_products,<br>
  SUM(productQuantity) AS total_quantity<br>
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` AS m,<br>
    UNNEST(hits) AS hits,<br>
    UNNEST(hits.product) AS product<br>
INNER JOIN u <br>
ON m.fullVisitorId = u.fullVisitorId <br>
WHERE productRevenue IS NOT NULL <br>
      AND v2ProductName != "YouTube Men's Vintage Henley" <br>
      AND _TABLE_SUFFIX BETWEEN '0701' AND '0731' <br>
GROUP BY v2ProductName <br>
ORDER BY total_quantity DESC; <br>

- Result<br>
<img width="351" alt="S9" src="https://github.com/user-attachments/assets/b3cb38ad-8111-480e-a662-34716068117d" /> <br>

**Query 08: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017** <br>
WITH product_data AS( <br>
SELECT<br>
    FORMAT_DATE('%Y%m', parse_date('%Y%m%d',date)) AS month,<br>
    COUNT(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) AS num_product_view,<br>
    COUNT(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) AS num_add_to_cart,<br>
    count(CASE WHEN eCommerceAction.action_type = '6' and product.productRevenue is not null THEN product.v2ProductName END) AS num_purchase<br>
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,<br>
 UNNEST(hits) AS hits,<br>
UNNEST (hits.product) AS product<br>
WHERE _table_suffix between '20170101' and '20170331'<br>
AND eCommerceAction.action_type in ('2','3','6')<br>
GROUP BY month<br>
ORDER BY month<br>
)<br>

SELECT<br>
    *,<br>
    ROUND(num_add_to_cart/num_product_view * 100, 2) AS add_to_cart_rate,<br>
    ROUND(num_purchase/num_product_view * 100, 2) AS purchase_rate<br>
FROM product_data;<br>
- Result<br>
  <img width="404" alt="S10" src="https://github.com/user-attachments/assets/7489778f-afe7-4bf6-8bd3-ae3cd0357b21" /><br>
