from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import datetime
import pandas as pd
from sqlalchemy import create_engine, text, types

# ── 연결 정보 ──────────────────────────────────────────────
# OLTP = 원천(운영) DB,  WH = 분석용 데이터웨어하우스
OLTP_URL = "postgresql+psycopg2://oltp_user:oltp_pass@postgres-oltp:5432/subscription_db"
WH_URL = "postgresql+psycopg2://warehouse_user:warehouse_pass@postgres-warehouse:5432/analytics_db"

oltp_engine = create_engine(OLTP_URL)
wh_engine = create_engine(WH_URL)

# raw 테이블을 만들 때 날짜 컬럼을 진짜 DATE 타입으로 강제하기 위한 매핑.
# 이게 없으면 pandas가 날짜를 TIMESTAMP로 넣어서 아래 SQL의 날짜 뺄셈이 이상해짐.
DATE_TYPES = {
    "signup_date": types.Date(),
    "start_date": types.Date(),
    "cancelled_at": types.Date(),
    "payment_date": types.Date(),
    "created_at": types.TIMESTAMP(),
}


# ==========================================================
# Task 1: 추출 + 적재 (OLTP → Warehouse raw_*)
#   XCom/JSON을 거치지 않고 DB에서 DB로 바로 옮긴다.
# ==========================================================
def extract_load(**context):
    for table in ["users", "subscriptions", "payments"]:
        df = pd.read_sql(text(f"SELECT * FROM {table}"), oltp_engine)
        df.to_sql(
            f"raw_{table}",
            wh_engine,
            if_exists="replace",          # 매 실행마다 raw_*를 새로 만듦(중복 방지)
            index=False,
            dtype={k: v for k, v in DATE_TYPES.items() if k in df.columns},
        )
        print(f"📥 {table}: {len(df)} rows → raw_{table}")


# ==========================================================
# Task 2: 변환 (raw_* → dim/fact)
#   원천 데이터를 분석하기 좋은 별 스키마(dim/fact)로 가공.
# ==========================================================
def transform_and_load(**context):
    with wh_engine.begin() as conn:
        # 차원(Dimension) 테이블
        conn.execute(text("DROP TABLE IF EXISTS dim_users"))
        conn.execute(text("""
            CREATE TABLE dim_users AS
            SELECT DISTINCT user_id, email, country, marketing_channel, signup_date
            FROM raw_users
        """))

        conn.execute(text("CREATE TABLE IF NOT EXISTS dim_plans (plan_type TEXT PRIMARY KEY, monthly_price INT)"))
        conn.execute(text("""
            INSERT INTO dim_plans VALUES ('Free', 0), ('Basic', 29000), ('Pro', 79000), ('Enterprise', 149000)
            ON CONFLICT (plan_type) DO NOTHING
        """))

        # 사실(Fact) 테이블
        conn.execute(text("DROP TABLE IF EXISTS fact_subscriptions"))
        conn.execute(text("""
            CREATE TABLE fact_subscriptions AS
            SELECT s.*,
                CASE WHEN s.cancelled_at IS NOT NULL THEN 1 ELSE 0 END AS is_churned,
                CASE WHEN s.cancelled_at IS NOT NULL THEN s.cancelled_at - s.start_date
                     ELSE CURRENT_DATE - s.start_date END AS subscription_days
            FROM raw_subscriptions s
        """))

        conn.execute(text("DROP TABLE IF EXISTS fact_payments"))
        conn.execute(text("""
            CREATE TABLE fact_payments AS
            SELECT p.*,
                CASE WHEN p.refund_flag THEN -p.amount ELSE p.amount END AS net_amount
            FROM raw_payments p
        """))
    print("✅ transform_and_load 완료")


# ==========================================================
# Task 3: 지표 계산 (fact/dim → mart_*)
# ==========================================================
def compute_metrics(**context):
    with wh_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS mart_mrr_daily"))
        conn.execute(text("""
            CREATE TABLE mart_mrr_daily AS
            SELECT p.payment_date AS date, fs.plan_type,
                COUNT(DISTINCT p.subscription_id) AS paying_subscriptions,
                SUM(p.net_amount) AS daily_revenue, AVG(p.net_amount) AS arpu
            FROM fact_payments p
            JOIN fact_subscriptions fs ON p.subscription_id = fs.subscription_id
            WHERE p.status = 'success'
            GROUP BY p.payment_date, fs.plan_type
        """))
        conn.execute(text("DROP TABLE IF EXISTS mart_churn_monthly"))
        conn.execute(text("""
            CREATE TABLE mart_churn_monthly AS
            SELECT DATE_TRUNC('month', start_date) AS month, plan_type,
                COUNT(*) AS new_subscriptions, SUM(is_churned) AS churned_subscriptions,
                ROUND(SUM(is_churned) * 100.0 / NULLIF(COUNT(*), 0), 2) AS churn_rate_pct
            FROM fact_subscriptions GROUP BY DATE_TRUNC('month', start_date), plan_type
        """))
        conn.execute(text("DROP TABLE IF EXISTS mart_revenue_by_country"))
        conn.execute(text("""
            CREATE TABLE mart_revenue_by_country AS
            SELECT u.country, DATE_TRUNC('month', p.payment_date) AS month,
                SUM(p.net_amount) AS revenue, COUNT(DISTINCT p.subscription_id) AS active_subscriptions
            FROM fact_payments p
            JOIN fact_subscriptions fs ON p.subscription_id = fs.subscription_id
            JOIN dim_users u ON fs.user_id = u.user_id
            WHERE p.status = 'success' GROUP BY u.country, DATE_TRUNC('month', p.payment_date)
        """))
        conn.execute(text("DROP TABLE IF EXISTS mart_user_cohort"))
        conn.execute(text("""
            CREATE TABLE mart_user_cohort AS
            SELECT DATE_TRUNC('month', signup_date) AS signup_month, marketing_channel,
                COUNT(*) AS total_users,
                COUNT(DISTINCT CASE WHEN CURRENT_DATE - signup_date <= 30 THEN user_id END) AS d30_active,
                COUNT(DISTINCT CASE WHEN CURRENT_DATE - signup_date <= 90 THEN user_id END) AS d90_active
            FROM dim_users GROUP BY DATE_TRUNC('month', signup_date), marketing_channel
        """))
    print("✅ compute_metrics 완료")


# ==========================================================
# Task 4: 검증
# ==========================================================
def sync_check(**context):
    with wh_engine.begin() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM mart_mrr_daily")).fetchone()
        print(f"✅ mart_mrr_daily: {result[0]} rows")


with DAG(
    dag_id="subscription_pipeline",
    default_args={"owner": "data-engineer", "retries": 1, "retry_delay": datetime.timedelta(minutes=5)},
    description="구독 서비스 데이터 파이프라인",
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["subscription", "etl"],
) as dag:
    t1 = PythonOperator(task_id="extract_load", python_callable=extract_load)
    t2 = PythonOperator(task_id="transform_and_load", python_callable=transform_and_load)
    t3 = PythonOperator(task_id="compute_metrics", python_callable=compute_metrics)
    t4 = PythonOperator(task_id="sync_check", python_callable=sync_check)
    t1 >> t2 >> t3 >> t4
