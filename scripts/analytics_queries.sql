-- ==========================================
-- 가짜 구독 서비스 분석 SQL 모음
-- DuckDB 또는 PostgreSQL 웨어하우스에서 실행 가능
-- ==========================================

-- 1. 최근 7일 일별 매출 (MRR 관점)
SELECT 
    date,
    SUM(daily_revenue) AS total_revenue,
    SUM(paying_subscriptions) AS total_subs
FROM mart_mrr_daily
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY date
ORDER BY date DESC;

-- 2. 요금제별 누적 매출
SELECT 
    plan_type,
    SUM(daily_revenue) AS total_revenue,
    AVG(arpu) AS avg_arpu
FROM mart_mrr_daily
GROUP BY plan_type
ORDER BY total_revenue DESC;

-- 3. 월별 Churn Rate
SELECT 
    month,
    plan_type,
    new_subscriptions,
    churned_subscriptions,
    churn_rate_pct
FROM mart_churn_monthly
ORDER BY month DESC, plan_type;

-- 4. 국가별 월별 매출 (지도 차트용)
SELECT 
    country,
    month,
    revenue,
    active_subscriptions
FROM mart_revenue_by_country
ORDER BY month DESC, revenue DESC;

-- 5. 마케팅 채널별 유저 획득 효율
SELECT 
    signup_month,
    marketing_channel,
    total_users,
    d30_active,
    ROUND(d30_active * 100.0 / total_users, 1) AS d30_retention_pct
FROM mart_user_cohort
ORDER BY signup_month DESC, total_users DESC;

-- 6. 평균 구독 유지 기간 (LTV 추정 기초)
SELECT 
    plan_type,
    ROUND(AVG(subscription_days), 1) AS avg_days,
    COUNT(*) AS total_subs
FROM fact_subscriptions
GROUP BY plan_type;

-- 7. 환불률 분석
SELECT 
    plan_type,
    COUNT(*) AS total_payments,
    SUM(CASE WHEN refund_flag THEN 1 ELSE 0 END) AS refunded_count,
    ROUND(SUM(CASE WHEN refund_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS refund_rate_pct
FROM fact_payments fp
JOIN fact_subscriptions fs ON fp.subscription_id = fs.subscription_id
GROUP BY plan_type;

-- 8. 신규 vs 재구매 매출 비율
WITH first_payment AS (
    SELECT 
        subscription_id,
        MIN(payment_date) AS first_pay_date
    FROM fact_payments
    WHERE status = 'success'
    GROUP BY subscription_id
)
SELECT 
    CASE 
        WHEN fp.payment_date = fp2.first_pay_date THEN 'First Payment'
        ELSE 'Renewal Payment'
    END AS payment_type,
    SUM(fp.net_amount) AS revenue,
    COUNT(*) AS payment_count
FROM fact_payments fp
JOIN first_payment fp2 ON fp.subscription_id = fp2.subscription_id
WHERE fp.status = 'success'
GROUP BY payment_type;
