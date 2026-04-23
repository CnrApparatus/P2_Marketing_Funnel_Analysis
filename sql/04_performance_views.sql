-- ========================================================
-- 9) CAMPAIGN PERFORMANCE
-- Full Performance by Campaign
-- ========================================================

CREATE VIEW v_campaign_performance AS 
SELECT
    campaign_id,
    SUM(impressions) AS total_impressions,
    SUM(clicks) AS total_clicks,
    SUM(spend) AS total_spend,
    ROUND(SUM(clicks)::numeric / NULLIF(SUM(impressions),0), 4) AS ctr,
    ROUND(SUM(spend)::numeric / NULLIF(SUM(clicks),0), 2) AS cpc
FROM clean_ad_performance_final
GROUP BY campaign_id;


-- ========================================================
-- 10) CHANNEL PERFORMANCE
-- Performance by Channel
-- ========================================================

CREATE VIEW v_channel_performance AS

WITH spend AS (
    SELECT
        campaign_id,
        SUM(spend) AS total_spend
    FROM clean_ad_performance_final
    GROUP BY campaign_id
),

revenue AS (
    SELECT
        campaign_id,
        SUM(revenue) AS total_revenue
    FROM clean_conversions
    GROUP BY campaign_id
),

campaign_level AS (
    SELECT
        c.campaign_id,
        c.channel,
        COALESCE(s.total_spend, 0) AS total_spend,
        COALESCE(r.total_revenue, 0) AS total_revenue
    FROM clean_ad_campaigns c
    LEFT JOIN spend s
        ON c.campaign_id = s.campaign_id
    LEFT JOIN revenue r
        ON c.campaign_id = r.campaign_id
)

SELECT
    channel,

    SUM(total_spend) AS total_spend,
    SUM(total_revenue) AS total_revenue,

    ROUND(
        (SUM(total_revenue) - SUM(total_spend)) /
        NULLIF(SUM(total_spend),0),
    2) AS roi,

    COUNT(DISTINCT campaign_id) AS campaigns

FROM campaign_level
GROUP BY channel;


-- ========================================================
-- 11) DEVICE PERFORMANCE
-- Performance by Device analysis
-- ========================================================

CREATE VIEW v_device_performance AS

WITH sessions AS (
    SELECT
        device,
        COUNT(DISTINCT session_id) AS sessions
    FROM clean_website_sessions
    GROUP BY device
),

conversions AS (
    SELECT
        s.device,
        COUNT(DISTINCT c.conversion_id) AS conversions
    FROM clean_conversions c
    JOIN clean_website_sessions s
        ON c.user_id = s.user_id
    GROUP BY s.device
)

SELECT
    s.device,
    s.sessions,
    COALESCE(c.conversions, 0) AS conversions,

    ROUND(
        COALESCE(c.conversions,0)::numeric /
        NULLIF(s.sessions,0),
    4) AS conversion_rate

FROM sessions s
LEFT JOIN conversions c
    ON s.device = c.device;


-- ========================================================
-- 12) COUNTRY ANALYSIS
-- Performance by Country analysis
-- ========================================================

CREATE VIEW v_country_analysis AS

WITH sessions AS (
    SELECT
        country,
        COUNT(DISTINCT session_id) AS sessions
    FROM clean_website_sessions
    GROUP BY country
),

conversions AS (
    SELECT
        s.country,
        COUNT(DISTINCT c.conversion_id) AS conversions
    FROM clean_conversions c
    JOIN clean_website_sessions s
        ON c.user_id = s.user_id
    GROUP BY s.country
)

SELECT
    s.country,
    s.sessions,
    COALESCE(c.conversions, 0) AS conversions,

    ROUND(
        COALESCE(c.conversions,0)::numeric /
        NULLIF(s.sessions,0),
    4) AS conversion_rate

FROM sessions s
LEFT JOIN conversions c
    ON s.country = c.country;


-- ========================================================
-- 13) BUDGET WASTE DETECTION
-- Performance by Budget analysis
-- ========================================================

CREATE VIEW v_budget_waste_detection AS

WITH spend AS (
    SELECT
        campaign_id,
        SUM(spend) AS total_spend
    FROM clean_ad_performance_final
    GROUP BY campaign_id
),

conversions AS (
    SELECT
        campaign_id,
        COUNT(DISTINCT conversion_id) AS conversions
    FROM clean_conversions
    GROUP BY campaign_id
),

campaign_data AS (
    SELECT
        s.campaign_id,
        s.total_spend,
        COALESCE(c.conversions, 0) AS conversions
    FROM spend s
    LEFT JOIN conversions c
        ON s.campaign_id = c.campaign_id
),

threshold AS (
    SELECT
        PERCENTILE_CONT(0.25) 
        WITHIN GROUP (ORDER BY conversions) AS p25_conversions
    FROM campaign_data
)

SELECT
    cd.campaign_id,
    cd.total_spend,
    cd.conversions
FROM campaign_data cd
CROSS JOIN threshold t

WHERE 
    cd.total_spend > 1000000
    AND cd.conversions < t.p25_conversions;
