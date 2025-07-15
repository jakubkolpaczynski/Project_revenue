# Game Revenue Analysis – SQL + Tableau Dashboard
## Project Overview
This project focuses on analyzing revenue and user behavior data from paid users of video games. Using SQL, I extracted key sales metrics and visualized them in an interactive Tableau dashboard.
The primary goals were:
- To calculate and monitor recurring revenue performance (MRR)
- To understand user retention and churn behavior
- To identify opportunities for revenue growth (expansion MRR) and risks (contraction/churned revenue)
## Technologies Used
- SQL (PostgreSQL) – for data preparation, aggregation, and metric calculation
- Tableau – for interactive dashboard development and visualization
## Key Metrics Calculated
The SQL query aggregates monthly revenue and user activity, including:
- MRR – Monthly Recurring Revenue
- New MRR – Revenue from new paying users
- Expansion MRR – Growth in revenue from existing users
- Contraction MRR – Revenue loss from downgrades
- Churned Revenue – Revenue lost from users who left
- ARPPU – Average Revenue Per Paying User
- Churn Rate – Proportion of users lost from the previous month
- Revenue Churn Rate – Proportion of MRR lost due to churn
## Dashboard Preview
The Tableau dashboard visualizes monthly revenue trends, churn behavior, user segments by language, and more.
<a href="https://public.tableau.com/app/profile/jakub.ko.paczy.ski/viz/projekt_dashboard_17240160849080/Dashboard1?publish=yes"> Click here for Tableau dashboard</a>
## Key Insights
- UK language users dominate paid user base, indicating they are the primary market segment.
- MRR showed a strong increase between June and October, suggesting possible seasonality or successful campaigns.
- High ARPPU in low-user months indicates strong monetization among loyal users.
- Churn rate spikes in Q4 (Sept–Dec) highlight retention challenges and possible user dissatisfaction or lifecycle issues.
- Expansion MRR doesn’t consistently offset contraction MRR, pointing to a need for better retention or upsell strategies.
- Revenue peaks in October and November may result from seasonal promotions or in-game events.
## Business Recommendations
Based on the analysis, here are actionable recommendations:
- Investigate churn causes, especially in September–December (e.g., user surveys, behavior tracking).
- Segment users by ARPPU and activity, and design targeted engagement or retention campaigns for each segment.
- Implement retention programs, such as loyalty rewards or special offers for at-risk or returning users.
- Focus on Expansion MRR by offering new premium features, in-game purchases, or bundled upgrades.
- Align marketing with growth periods (e.g., summer/fall), leveraging key revenue momentum.
    Enhance the dashboard with churn prediction models (e.g., machine learning) or automated alerts in Tableau.
