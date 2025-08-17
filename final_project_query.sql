WITH joined AS (
    SELECT
        gpu.user_id,
        gpu.game_name,
        gpu.language,
        gpu.has_older_device_model,
        gpu.age,
        gp.payment_date,
        gp.revenue_amount_usd
    FROM games_paid_users gpu
    INNER JOIN games_payments gp ON gpu.user_id = gp.user_id
),
dates AS (
    SELECT
        user_id,
        MIN(payment_date) AS first_payment_date,
        MAX(payment_date) AS last_payment_date
    FROM games_payments
    GROUP BY user_id
),
monthly_data AS (
    select
        joined.user_id,
        EXTRACT(MONTH FROM joined.payment_date) AS month,
        joined.language,
        joined.age,
        COUNT(joined.user_id) AS total_users,
        -- Count of Paid Users calculation
        COUNT(DISTINCT joined.user_id) AS paid_users,
        -- Total Revenue calculation
        SUM(joined.revenue_amount_usd) AS total_revenue,
        -- ARPPU calculation
        ROUND(SUM(joined.revenue_amount_usd) / NULLIF(COUNT(DISTINCT joined.user_id), 0), 2) AS ARPPU,
        -- MRR calculations
        ROUND(COUNT(joined.user_id) * (SUM(joined.revenue_amount_usd) / NULLIF(COUNT(DISTINCT joined.user_id), 0)), 2) AS MRR,
        -- New Paid Users calculations
        COUNT(DISTINCT CASE
                          WHEN EXTRACT(MONTH FROM dates.first_payment_date) = EXTRACT(MONTH FROM joined.payment_date)
                          THEN dates.user_id
                          ELSE NULL
                       END) AS new_paid_users,
        -- New Users Revenue calculations
        SUM(CASE
                WHEN EXTRACT(MONTH FROM dates.first_payment_date) = EXTRACT(MONTH FROM joined.payment_date)
                THEN joined.revenue_amount_usd
                ELSE 0
            END) AS revenue_new_users,
        -- New MRR calculations
        ROUND(
            (SUM(CASE
                    WHEN EXTRACT(MONTH FROM dates.first_payment_date) = EXTRACT(MONTH FROM joined.payment_date)
                    THEN joined.revenue_amount_usd
                    ELSE 0
                 END) /
             NULLIF(COUNT(DISTINCT CASE
                                    WHEN EXTRACT(MONTH FROM dates.first_payment_date) = EXTRACT(MONTH FROM joined.payment_date)
                                    THEN joined.user_id
                                    ELSE NULL
                                  END), 0))
            * COUNT(CASE
                        WHEN EXTRACT(MONTH FROM dates.first_payment_date) = EXTRACT(MONTH FROM joined.payment_date)
                        THEN joined.user_id
                        ELSE NULL
                     END), 2) AS new_mrr,
        -- Churned Users calculations
        COUNT(DISTINCT CASE
                        WHEN EXTRACT(MONTH FROM dates.last_payment_date) = EXTRACT(MONTH FROM joined.payment_date)
                        THEN dates.user_id
                        ELSE NULL
                      END) AS churned_users,
        COALESCE(LAG(COUNT(DISTINCT joined.user_id), 1) OVER (PARTITION BY joined.user_id, joined.language, joined.age ORDER BY EXTRACT(MONTH FROM joined.payment_date)), 0) AS previous_paid_users,
        -- Expansion MRR calculation
        ROUND(
            CASE
                WHEN COUNT(joined.user_id) * (SUM(joined.revenue_amount_usd) / NULLIF(COUNT(DISTINCT joined.user_id), 0)) > 
                     LAG(COUNT(joined.user_id) * (SUM(joined.revenue_amount_usd) / NULLIF(COUNT(DISTINCT joined.user_id), 0)), 1) OVER (PARTITION BY joined.user_id, joined.language, joined.age ORDER BY EXTRACT(MONTH FROM joined.payment_date))
                THEN COUNT(joined.user_id) * (SUM(joined.revenue_amount_usd) / NULLIF(COUNT(DISTINCT joined.user_id), 0)) - 
                     LAG(COUNT(joined.user_id) * (SUM(joined.revenue_amount_usd) / NULLIF(COUNT(DISTINCT joined.user_id), 0)), 1) OVER (PARTITION BY joined.user_id, joined.language, joined.age ORDER BY EXTRACT(MONTH FROM joined.payment_date))
                ELSE 0
            END, 2) AS expansion_mrr,
        -- Contraction MRR calculation
        ROUND(
            CASE
                WHEN COUNT(joined.user_id) * (SUM(joined.revenue_amount_usd) / NULLIF(COUNT(DISTINCT joined.user_id), 0)) < 
                     LAG(COUNT(joined.user_id) * (SUM(joined.revenue_amount_usd) / NULLIF(COUNT(DISTINCT joined.user_id), 0)), 1) OVER (PARTITION BY joined.user_id, joined.language, joined.age ORDER BY EXTRACT(MONTH FROM joined.payment_date))
                THEN LAG(COUNT(joined.user_id) * (SUM(joined.revenue_amount_usd) / NULLIF(COUNT(DISTINCT joined.user_id), 0)), 1) OVER (PARTITION BY joined.user_id, joined.language, joined.age ORDER BY EXTRACT(MONTH FROM joined.payment_date)) - 
                     COUNT(joined.user_id) * (SUM(joined.revenue_amount_usd) / NULLIF(COUNT(DISTINCT joined.user_id), 0))
                ELSE 0
            END, 2) AS contraction_mrr
    FROM
        joined
    INNER JOIN dates ON joined.user_id = dates.user_id
    GROUP BY
        month, joined.user_id, dates.user_id, dates.first_payment_date, dates.last_payment_date, joined.language, joined.age
    ORDER BY
        month
)
SELECT
    monthly_data.user_id,
    month,
    language,
    age,
    total_users,
    paid_users,
    total_revenue,
    arppu,
    mrr,
    new_paid_users,
    revenue_new_users,
    new_mrr,
    churned_users,
    previous_paid_users,
    -- Churn Rate calculations
    COALESCE(churned_users / LAG(paid_users, 1) OVER (PARTITION BY language, age, month), 0) AS churn_rate,
    -- Churned Revenue calculations
    CASE
        WHEN previous_paid_users = 1
        THEN total_revenue
        ELSE 0
    END AS churned_revenue,
    -- Revenue Churn rate calculations (churned_revenue / previous_mrr)
    COALESCE(CASE
                 WHEN previous_paid_users = 1
                 THEN total_revenue
                 ELSE 0
             END /
             LAG(mrr, 1) OVER (PARTITION BY month), 0) AS revenue_churn_rate,
    expansion_mrr,
    contraction_mrr
FROM
    monthly_data, dates
GROUP BY
    month, monthly_data.user_id, language, age, total_users, paid_users, total_revenue, arppu, mrr, new_paid_users, revenue_new_users, new_mrr, churned_users, previous_paid_users, expansion_mrr, contraction_mrr
ORDER BY
    month;
