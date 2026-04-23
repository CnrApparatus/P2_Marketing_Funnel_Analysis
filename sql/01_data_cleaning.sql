-- ========================================================
-- PROJECT: Revenue Leakage & Marketing Funnel Audit
--          D2C Electronics Brand (India)
--
-- SCOPE:   End-to-End SQL Data Cleaning, Standardization
--          & Validation Pipeline
--
-- OUTPUT:  Transforming Raw Marketing Data into Reliable,
--          Analysis-Ready Tables
-- ========================================================


-- ========================================================
-- 1) DUPLICATE CHECKS
-- Validate primary key uniqueness across core tables
-- ========================================================

SELECT campaign_id, COUNT(*)
FROM ad_campaigns AS camp
GROUP BY campaign_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

SELECT session_id, COUNT(*)
FROM website_sessions
GROUP BY session_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

SELECT user_id, COUNT(*)
FROM conversions
GROUP BY user_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

SELECT lead_id, COUNT(*)
FROM leads
GROUP BY lead_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;

SELECT conversion_id, COUNT(*)
FROM conversions
GROUP BY conversion_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;


-- ========================================================
-- 2) DUPLICATE REMOVAL (CREATE CLEAN TABLES)
-- Preserve raw data by creating deduplicated versions
-- ========================================================

CREATE TABLE clean_ad_campaigns AS
SELECT DISTINCT ON (campaign_id) *
FROM ad_campaigns
ORDER BY campaign_id;

CREATE TABLE clean_website_sessions AS
SELECT DISTINCT ON (session_id) *
FROM website_sessions
ORDER BY session_id;

CREATE TABLE clean_leads AS
SELECT DISTINCT ON (lead_id) *
FROM leads
ORDER BY lead_id;

CREATE TABLE clean_conversions AS
SELECT DISTINCT ON (conversion_id) *
FROM conversions
ORDER BY conversion_id;


-- ========================================================
-- 3) NULL HANDLING & DATA CORRECTIONS
-- Fix missing values and apply manual corrections
-- ========================================================

-- Remove NULL primary keys
DELETE FROM clean_ad_campaigns
WHERE campaign_id IS NULL;

DELETE FROM clean_conversions
WHERE user_id IS NULL;

DELETE FROM clean_website_sessions
WHERE session_date IS NULL;


-- Manual channel fixes (based on campaign patterns)
UPDATE clean_ad_campaigns
SET channel = 'email'
WHERE id = 77;

UPDATE clean_ad_campaigns
SET channel = 'meta'
WHERE id = 80;


-- Clean ad performance table
DELETE FROM ad_performance
WHERE campaign_id IS NULL;

UPDATE ad_performance
SET impressions = COALESCE(impressions, 0),
    clicks      = COALESCE(clicks, 0),
    cost        = COALESCE(cost, 0);


-- Rescue missing session dates using conversion data
UPDATE clean_website_sessions AS s
SET session_date = c.conversion_date
FROM clean_conversions AS c
WHERE s.user_id = c.user_id
  AND s.session_date IS NULL
  AND c.conversion_date IS NOT NULL;


-- Traffic source classification
ALTER TABLE clean_website_sessions
ADD COLUMN traffic_source TEXT;

UPDATE clean_website_sessions
SET traffic_source = 'paid'
WHERE campaign_id IS NOT NULL;

UPDATE clean_website_sessions
SET traffic_source = 'organic'
WHERE campaign_id IS NULL;


-- Fill missing country
UPDATE clean_website_sessions
SET country = 'unknown'
WHERE country IS NULL;


-- Rescue missing lead dates
UPDATE clean_leads AS l
SET lead_date = c.conversion_date
FROM clean_conversions AS c
WHERE l.user_id = c.user_id
  AND l.lead_date IS NULL
  AND c.conversion_date IS NOT NULL;


-- Fix NULL revenue
UPDATE clean_conversions
SET revenue = 0
WHERE revenue IS NULL;
