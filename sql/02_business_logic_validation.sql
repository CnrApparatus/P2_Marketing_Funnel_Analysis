-- ========================================================
-- 5) STANDARDIZATION (CATEGORICAL CLEANING)
-- Normalize device and country values
-- ========================================================

-- DEVICE STANDARDIZATION
SELECT DISTINCT device FROM clean_website_sessions;

UPDATE clean_website_sessions
SET device = 'mobile'
WHERE device IN ('MOBILE', 'Mobile', 'mob', 'mobile ', 'phone');

UPDATE clean_website_sessions
SET device = 'desktop'
WHERE device IN ('Desktop', 'PC', 'pc');

UPDATE clean_website_sessions
SET device = 'tablet'
WHERE device IN ('TAB', 'Tablet');

UPDATE clean_website_sessions
SET device = 'bot'
WHERE device IN ('BOT', 'crawler');


-- COUNTRY STANDARDIZATION
SELECT DISTINCT country FROM clean_website_sessions;

UPDATE clean_website_sessions
SET country = 'India'
WHERE country IN ('Bharat', 'IN', 'IND', 'in ', 'india');


-- ========================================================
-- 6) METRIC VALIDATION
-- Ensure logical consistency in performance data
-- ========================================================

CREATE TABLE clean_ad_performance AS
SELECT
    id,
    date AS performance_date,
    campaign_id,
    impressions,
    LEAST(clicks, impressions) AS clicks,
    cost AS spend
FROM ad_performance;

SELECT *
FROM clean_ad_performance
WHERE impressions < 0 OR clicks < 0;


-- ========================================================
-- 7) TEMPORAL VALIDATION
-- Ensure time consistency across funnel stages
-- ========================================================

-- Fix incorrect campaign_id tagging
UPDATE clean_website_sessions
SET campaign_id = NULL
WHERE campaign_id = 'organic';


-- Remove invalid conversion timelines
DELETE FROM clean_conversions AS c
USING clean_leads AS l
WHERE c.user_id = l.user_id
  AND c.conversion_date < l.lead_date;


-- Remove sessions outside campaign duration
UPDATE clean_website_sessions AS s
SET campaign_id = NULL
FROM clean_ad_campaigns AS c
WHERE s.campaign_id = c.campaign_id
  AND (
        s.session_date < c.start_date
     OR s.session_date > c.end_date
  );
