-- ========================================================
-- 8) FINAL CLEANING & FEATURE ENGINEERING
-- Handle outliers and user classification
-- ========================================================

-- Revenue outlier flag
ALTER TABLE clean_conversions
ADD COLUMN revenue_outlier BOOLEAN;

UPDATE clean_conversions
SET revenue_outlier = TRUE
WHERE revenue > 5000;

UPDATE clean_conversions
SET revenue_outlier = FALSE
WHERE revenue <= 5000;


-- Remove missing conversion dates
DELETE FROM clean_conversions
WHERE conversion_date IS NULL;


-- Fix user_id formatting
UPDATE clean_conversions
SET user_id = REPLACE(user_id, 'user-', '')
WHERE user_id LIKE 'user-U%';


-- User type classification IN CONVERSIONS
ALTER TABLE clean_conversions
ADD COLUMN user_type TEXT;

UPDATE clean_conversions
SET user_type =
CASE
    WHEN user_id LIKE 'BOT%' THEN 'bot'
    WHEN user_id LIKE 'U_DIRECT%' THEN 'direct'
    WHEN user_id LIKE 'U_NEW%' THEN 'new_user'
    WHEN user_id ~ '^U[0-9]{6}$' THEN 'normal'
    ELSE 'unknown'
END;


-- Remove unknown users IN CONVERSIONS
DELETE FROM clean_conversions
WHERE user_type = 'unknown';


-- User type classification IN LEADS
UPDATE clean_leads
SET user_id = REPLACE(user_id, 'UID_', '')
WHERE user_id LIKE 'UID_U%';

UPDATE clean_leads
SET user_id = REPLACE(user_id, 'user-', '')
WHERE user_id LIKE 'user-U%';

ALTER TABLE clean_leads
ADD COLUMN user_type TEXT;

UPDATE clean_leads
SET user_type =
CASE
    WHEN user_id LIKE 'BOT%' THEN 'bot'
    WHEN user_id LIKE 'U_NEW%' THEN 'new_user'
    WHEN user_id ~ '^U[0-9]{6}$' THEN 'normal'
    ELSE 'unknown'
END;


-- Remove unknown IN LEADS
DELETE FROM clean_leads
WHERE user_type = 'unknown';


-- User type classification IN SESSIONS
UPDATE clean_website_sessions
SET user_id = REPLACE(user_id, 'UID_', '')
WHERE user_id LIKE 'UID_U%';

UPDATE clean_website_sessions
SET user_id = REPLACE(user_id, 'user-', '')
WHERE user_id LIKE 'user-U%';


ALTER TABLE clean_website_sessions
ADD COLUMN user_type TEXT;

UPDATE clean_website_sessions
SET user_type =
CASE
    WHEN user_id LIKE 'BOT%' THEN 'bot'
    WHEN user_id ~ '^U[0-9]{6}$' THEN 'normal'
    ELSE 'unknown'
END;


-- Remove unknown users IN SESSIONS
DELETE FROM clean_website_sessions
WHERE user_type = 'unknown';


-- Resetting primary keys
ALTER TABLE clean_ad_campaigns
ADD PRIMARY KEY (campaign_id);

ALTER TABLE clean_ad_performance
ADD PRIMARY KEY (campaign_id, performance_date);

ALTER TABLE clean_website_sessions
ADD PRIMARY KEY (session_id);

ALTER TABLE clean_leads
ADD PRIMARY KEY (lead_id);

ALTER TABLE clean_conversions
ADD PRIMARY KEY (conversion_id);


-- Dropping old SERIAL IDs
ALTER TABLE clean_ad_campaigns
DROP COLUMN id;

ALTER TABLE clean_website_sessions
DROP COLUMN id;

ALTER TABLE clean_leads
DROP COLUMN id;

ALTER TABLE clean_conversions
DROP COLUMN id;

SELECT *
FROM clean_conversions;


-- Deduping and cleaning of Performance Table
SELECT 
    campaign_id,
    performance_date,
    impressions,
    clicks,
    spend,
    COUNT(*) AS duplicate_count
FROM clean_ad_performance
GROUP BY 
    campaign_id,
    performance_date,
    impressions,
    clicks,
    spend
HAVING COUNT(*) > 1;

-- Creating Dedup table (301389)
CREATE TABLE clean_ad_performance_dedup AS
SELECT DISTINCT *
FROM clean_ad_performance;


-- Creating clean_ad_performance_final (22743)
CREATE TABLE clean_ad_performance_final AS
SELECT campaign_id,
	   performance_date,
	   SUM(impressions) AS impressions,
	   SUM(clicks) AS clicks,
	   SUM(spend) AS spend
FROM clean_ad_performance_dedup
GROUP BY campaign_id, performance_date;

SELECT *
FROM clean_ad_performance_final;

-- Deleting nulls from performance_date (120)
DELETE FROM clean_ad_performance_final
WHERE performance_date IS NULL;

ALTER TABLE clean_ad_performance_final
ADD PRIMARY KEY (campaign_id, performance_date);


-- INDEXING

-- performance table
CREATE INDEX idx_campaign_perf
ON clean_ad_performance_final(campaign_id);

-- sessions joins
CREATE INDEX idx_sessions_user
ON clean_website_sessions(user_id);

CREATE INDEX idx_sessions_campaign
ON clean_website_sessions(campaign_id);

-- leads joins
CREATE INDEX idx_leads_user
ON clean_leads(user_id);

-- conversions joins
CREATE INDEX idx_conv_user
ON clean_conversions(user_id);

CREATE INDEX idx_conv_campaign
ON clean_conversions(campaign_id);
