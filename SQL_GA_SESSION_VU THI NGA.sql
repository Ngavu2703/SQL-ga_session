--Query 1
SELECT
  SUM(totals.visits) AS visits,
  SUM(totals.pageviews) AS pageviews,
  SUM(totals.transactions) AS transactions,
  FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _TABLE_SUFFIX BETWEEN '0101' AND '0331'
GROUP BY month
ORDER BY month;

--Query 2
SELECT
  trafficSource.`source` AS sources,
  SUM(totals.bounces) AS total_no_of_bounces,
  SUM(totals.visits) AS total_visits,
  ROUND((100* SUM(totals.bounces)/SUM(totals.visits)),3) AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _TABLE_SUFFIX BETWEEN '0701' AND '0731'
GROUP BY trafficSource.`source`
ORDER BY total_visits DESC;

--Query 3
WITH 
month_data c(
  SELECT
    "Month" AS time_type,
    format_date("%Y%m", parse_date("%Y%m%d", date)) AS month,
    trafficSource.source AS source,
    SUM(p.productRevenue)/1000000 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
    unnest(hits) hits,
    unnest(product) p
  WHERE p.productRevenue is not null
  GROUP BY 1,2,3
  order by revenue DESC
),

week_data AS(
  SELECT
    "Week" AS time_type,
    format_date("%Y%W", parse_date("%Y%m%d", date)) AS week,
    trafficSource.source AS source,
    SUM(p.productRevenue)/1000000 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
    unnest(hits) hits,
    unnest(product) p
  WHERE p.productRevenue is not null
  GROUP BY 1,2,3
  order by revenue DESC
)

SELECT * 
FROM month_data
UNION ALL
SELECT * 
FROM week_data;
 
--Query 4
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

--Query 5
SELECT
    FORMAT_DATE("%Y%m",parse_date("%Y%m%d",date)) as month,
      SUM(totals.transactions)/count(distinct fullvisitorid) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST (hits) hits,
    UNNEST(product) product
WHERE  totals.transactions>=1
AND product.productRevenue is not null
GROUP BY month;

--Query 6
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

--Query 7
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

--Query 8
WITH product_data AS(
SELECT
    FORMAT_DATE('%Y%m', parse_date('%Y%m%d',date)) AS month,
    COUNT(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) AS num_product_view,
    COUNT(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) AS num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' AND product.productRevenue is not null THEN product.v2ProductName END) AS num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS hits,
  UNNEST (hits.product) AS product
WHERE _table_suffix BETWEEN '20170101' AND '20170331'
  AND eCommerceAction.action_type IN ('2','3','6')
GROUP BY month
ORDER BY month
)

SELECT
    *,
    ROUND(num_add_to_cart/num_product_view * 100, 2) AS add_to_cart_rate,
    ROUND(num_purchase/num_product_view * 100, 2) AS purchase_rate
FROM product_data;