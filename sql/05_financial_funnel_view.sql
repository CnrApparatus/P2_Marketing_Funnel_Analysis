-- ========================================================
-- 14) FUNNEL ANALYSIS
-- Full Funnel by Campaign
-- ========================================================

CREATE VIEW v_funnel_analysis AS

WITH sessions AS (
    SELECT
        campaign_id,
        COUNT(DISTINCT session_id) AS sessions
    FROM clean_website_sessions
    WHERE traffic_source = 'paid'
    GROUP BY campaign_id
),

leads AS (
    SELECT
        campaign_id,
        COUNT(DISTINCT lead_id) AS leads
    FROM clean_leads
    WHERE campaign_id IS NOT NULL
    GROUP BY campaign_id
),

conversions AS (
    SELECT
        campaign_id,
        COUNT(DISTINCT conversion_id) AS conversions
    FROM clean_conversions
    WHERE campaign_id IS NOT NULL
    GROUP BY campaign_id
)

SELECT
    p.campaign_id,
    SUM(p.impressions) AS impressions,
    SUM(p.clicks) AS clicks,

    COALESCE(s.sessions, 0) AS sessions,
    COALESCE(l.leads, 0) AS leads,
    COALESCE(c.conversions, 0) AS conversions,

    -- 🔥 Funnel Conversion Rates
    ROUND(COALESCE(s.sessions,0)::numeric / NULLIF(SUM(p.clicks),0), 4) AS click_to_session,
    ROUND(COALESCE(l.leads,0)::numeric / NULLIF(COALESCE(s.sessions,0),0), 4) AS session_to_lead,
    ROUND(COALESCE(c.conversions,0)::numeric / NULLIF(COALESCE(l.leads,0),0), 4) AS lead_to_conversion

FROM clean_ad_performance_final p

LEFT JOIN sessions s ON p.campaign_id = s.campaign_id
LEFT JOIN leads l ON p.campaign_id = l.campaign_id
LEFT JOIN conversions c ON p.campaign_id = c.campaign_id

GROUP BY 
    p.campaign_id, 
    s.sessions, 
    l.leads, 
    c.conversions;
    

-- ========================================================
-- 15) ROI ANALYSIS
-- ROI by Campaign
-- ========================================================

CREATE OR REPLACE VIEW v_campaign_roi AS

WITH revenue AS (
    SELECT
        campaign_id,
        SUM(revenue) AS total_revenue
    FROM clean_conversions
    GROUP BY campaign_id
)

SELECT
    c.campaign_id,
    c.campaign_name,
    c.channel,

    SUM(p.spend) AS total_spend,
    COALESCE(r.total_revenue, 0) AS total_revenue,

    ROUND(
        (COALESCE(r.total_revenue,0) - SUM(p.spend)) /
        NULLIF(SUM(p.spend),0),
    2) AS roi

FROM clean_ad_campaigns c

LEFT JOIN clean_ad_performance_final p
    ON c.campaign_id = p.campaign_id

LEFT JOIN revenue r
    ON c.campaign_id = r.campaign_id

GROUP BY c.campaign_id, c.campaign_name, c.channel, r.total_revenue;


-- ========================================================
-- 16) CAC ANALYSIS
-- CAC by Campaign
-- ========================================================

CREATE VIEW v_cac_analysis AS

WITH conversions AS (
    SELECT
        campaign_id,
        COUNT(DISTINCT conversion_id) AS customers
    FROM clean_conversions
    GROUP BY campaign_id
)

SELECT
    p.campaign_id,
    SUM(p.spend) AS total_spend,
    COALESCE(c.customers, 0) AS customers,

    ROUND(SUM(p.spend) / NULLIF(c.customers,0), 2) AS cac

FROM clean_ad_performance_final p

LEFT JOIN conversions c
    ON p.campaign_id = c.campaign_id
    
GROUP BY p.campaign_id, c.customers;


-- ========================================================
-- 17) CAMPAIGN ROAS & MATRIX EFFICIENCY
-- Overall matrix efficiency
-- ========================================================

CREATE VIEW v_campaign_roas_matrix AS

-- Pre-Aggregation

WITH performance AS (
    SELECT
        campaign_id,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend
    FROM clean_ad_performance_final
    GROUP BY campaign_id
),

sessions AS (
    SELECT
        campaign_id,
        COUNT(DISTINCT session_id) AS sessions
    FROM clean_website_sessions
    WHERE campaign_id IS NOT NULL
    GROUP BY campaign_id
),

leads AS (
    SELECT
        campaign_id,
        COUNT(DISTINCT lead_id) AS leads
    FROM clean_leads
    WHERE campaign_id IS NOT NULL
    GROUP BY campaign_id
),

conversions AS (
    SELECT
        campaign_id,
        COUNT(DISTINCT conversion_id) AS conversions,
        SUM(revenue) AS revenue   -- assuming revenue exists
    FROM clean_conversions
    GROUP BY campaign_id
),

final AS (
    SELECT
        p.campaign_id,
        p.impressions,
        p.clicks,
        p.spend,

        COALESCE(s.sessions, 0) AS sessions,
        COALESCE(l.leads, 0) AS leads,
        COALESCE(c.conversions, 0) AS conversions,
        COALESCE(c.revenue, 0) AS revenue

    FROM performance p
    LEFT JOIN sessions s ON p.campaign_id = s.campaign_id
    LEFT JOIN leads l ON p.campaign_id = l.campaign_id
    LEFT JOIN conversions c ON p.campaign_id = c.campaign_id
)


SELECT
    campaign_id,

    spend,
    revenue,
    leads,
    conversions,

    -- Core Metrics
    ROUND(revenue / NULLIF(spend, 0), 2) AS roas,
    ROUND(spend / NULLIF(leads, 0), 2) AS cpl,
    ROUND(spend / NULLIF(conversions, 0), 2) AS cac,

    -- Funnel Efficiency
    ROUND(clicks * 1.0 / NULLIF(impressions, 0), 4) AS ctr,
    ROUND(conversions * 1.0 / NULLIF(clicks, 0), 4) AS conversion_rate,

    -- Drop-offs
    ROUND(1 - (sessions * 1.0 / NULLIF(clicks, 0)), 4) AS drop_click_to_session,
    ROUND(1 - (leads * 1.0 / NULLIF(sessions, 0)), 4) AS drop_session_to_lead,
    ROUND(1 - (conversions * 1.0 / NULLIF(leads, 0)), 4) AS drop_lead_to_conversion

FROM final;



-- ========================================================
-- 18) MARKETING MASTER TABLE
-- all matrix combined
-- ========================================================

CREATE VIEW v_marketing_master_table AS

SELECT
	ac.campaign_id,
	ac.channel,
	
	-- volume
	f.impressions,
	f.clicks,
	f.sessions,
	f.leads,
	f.conversions,
	
	-- money
	rm.spend,
	rm.revenue,
	
	-- performance
	rm.ctr,
	c.cpc,
	rm.roas,
	roi.roi,
	rm.cpl,
	rm.cac,
	
	-- funnel
	f.click_to_session,
	f.session_to_lead,
	f.lead_to_conversion,
	
	-- drop-offs
	rm.drop_click_to_session,
	rm.drop_session_to_lead,
	rm.drop_lead_to_conversion

FROM clean_ad_campaigns as ac

JOIN v_campaign_performance as c
  ON ac.campaign_id = c.campaign_id

JOIN v_funnel_analysis as f
  ON ac.campaign_id = f.campaign_id
  
JOIN v_campaign_roi as roi
  ON ac.campaign_id = roi.campaign_id
  
JOIN v_campaign_roas_matrix as rm
  ON ac.campaign_id = rm.campaign_id;
  

-- ========================================================
-- 19) TABLEU TABLE
-- all Tableau used tables
-- ========================================================

CREATE VIEW v_marketing_funnel AS

WITH base AS (
    SELECT
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(sessions) AS sessions,
        SUM(leads) AS leads,
        SUM(conversions) AS conversions
    FROM v_marketing_master_table
)

SELECT 'Impressions' AS stage, impressions AS value, 1 AS stage_order FROM base
UNION ALL
SELECT 'Clicks', clicks, 2 FROM base
UNION ALL
SELECT 'Sessions', sessions, 3 FROM base
UNION ALL
SELECT 'Leads', leads, 4 FROM base
UNION ALL
SELECT 'Conversions', conversions, 5 FROM base

ORDER BY stage_order;
