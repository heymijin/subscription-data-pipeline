#!/usr/bin/env python3
"""
구독 서비스 합성 데이터 생성기
- 과거 90일치 데이터를 한 번에 생성
- 이후 매일 실행 시 하루치 데이터 추가
"""
import os
import random
import datetime
import psycopg2
from faker import Faker

fake = Faker()

# DB 연결 정보
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres-oltp"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "subscription_db"),
    "user": os.getenv("DB_USER", "oltp_user"),
    "password": os.getenv("DB_PASSWORD", "oltp_pass"),
}

PLANS = ["Free", "Basic", "Pro", "Enterprise"]
PLAN_PRICES = {"Free": 0, "Basic": 29000, "Pro": 79000, "Enterprise": 149000}
COUNTRIES = ["KR", "US", "JP", "DE", "GB", "FR", "CA", "AU"]
CHANNELS = ["Organic", "Google Ads", "Facebook", "Referral", "Offline"]
PAYMENT_STATUSES = ["success", "success", "success", "success", "failed", "refunded"]


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def init_tables(conn):
    """초기 테이블 생성"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id SERIAL PRIMARY KEY,
                email VARCHAR(255) NOT NULL,
                country VARCHAR(10),
                marketing_channel VARCHAR(50),
                signup_date DATE NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                subscription_id SERIAL PRIMARY KEY,
                user_id INT REFERENCES users(user_id),
                plan_type VARCHAR(50) NOT NULL,
                start_date DATE NOT NULL,
                status VARCHAR(20) DEFAULT 'active',
                cancelled_at DATE,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                payment_id SERIAL PRIMARY KEY,
                subscription_id INT REFERENCES subscriptions(subscription_id),
                amount INT NOT NULL,
                payment_date DATE NOT NULL,
                status VARCHAR(20) DEFAULT 'success',
                refund_flag BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
    conn.commit()


def generate_day_data(conn, target_date: datetime.date, num_new_users: int = 50):
    """특정 날짜의 데이터 생성"""
    with conn.cursor() as cur:
        # 1. 신규 유저 생성
        new_user_ids = []
        for _ in range(num_new_users):
            email = fake.email()
            country = random.choice(COUNTRIES)
            channel = random.choice(CHANNELS)
            cur.execute(
                """
                INSERT INTO users (email, country, marketing_channel, signup_date)
                VALUES (%s, %s, %s, %s)
                RETURNING user_id;
                """,
                (email, country, channel, target_date),
            )
            new_user_ids.append(cur.fetchone()[0])

        # 2. 구독 생성 (신규 유저 중 70%가 유료 구독)
        new_sub_ids = []
        for uid in new_user_ids:
            if random.random() < 0.7:
                plan = random.choice(PLANS[1:])  # Free 제외
                cur.execute(
                    """
                    INSERT INTO subscriptions (user_id, plan_type, start_date, status)
                    VALUES (%s, %s, %s, 'active')
                    RETURNING subscription_id;
                    """,
                    (uid, plan, target_date),
                )
                new_sub_ids.append(cur.fetchone()[0])

        # 3. 기존 구독 중 일부 해지 (churn)
        # 30일 이상 된 구독 중 2~5% 해지
        cur.execute("""
            SELECT subscription_id, plan_type, start_date
            FROM subscriptions
            WHERE status = 'active'
              AND start_date <= %s - INTERVAL '30 days'
              AND cancelled_at IS NULL;
        """, (target_date,))
        active_subs = cur.fetchall()

        churn_targets = random.sample(
            active_subs, k=min(len(active_subs), max(1, int(len(active_subs) * 0.03)))
        )
        for sub_id, _, _ in churn_targets:
            cur.execute(
                "UPDATE subscriptions SET status = 'cancelled', cancelled_at = %s WHERE subscription_id = %s;",
                (target_date, sub_id),
            )

        # 4. 결제 생성
        # (a) 신규 구독의 첫 결제
        for sub_id in new_sub_ids:
            cur.execute("SELECT plan_type FROM subscriptions WHERE subscription_id = %s;", (sub_id,))
            plan = cur.fetchone()[0]
            amount = PLAN_PRICES[plan]
            status = random.choice(PAYMENT_STATUSES)
            refund = status == "refunded"
            if status == "refunded":
                status = "success"  # 일단 결제는 됐다가 환불
            cur.execute(
                """
                INSERT INTO payments (subscription_id, amount, payment_date, status, refund_flag)
                VALUES (%s, %s, %s, %s, %s);
                """,
                (sub_id, amount, target_date, status, refund),
            )

        # (b) 기존 구독의 월별 재결제 (구독 시작일과 같은 날짜에 결제)
        cur.execute("""
            SELECT s.subscription_id, s.plan_type, s.start_date
            FROM subscriptions s
            WHERE s.status = 'active'
              AND EXTRACT(DAY FROM s.start_date) = %s
              AND s.start_date < %s;
        """, (target_date.day, target_date))
        renewals = cur.fetchall()

        for sub_id, plan, start_date in renewals:
            amount = PLAN_PRICES[plan]
            status = random.choice(PAYMENT_STATUSES)
            refund = status == "refunded"
            if status == "refunded":
                status = "success"
            cur.execute(
                """
                INSERT INTO payments (subscription_id, amount, payment_date, status, refund_flag)
                VALUES (%s, %s, %s, %s, %s);
                """,
                (sub_id, amount, target_date, status, refund),
            )

    conn.commit()
    print(f"✅ {target_date} 데이터 생성 완료 (신규유저 {num_new_users}명, 신규구독 {len(new_sub_ids)}개, 해지 {len(churn_targets)}개, 결제 {len(new_sub_ids) + len(renewals)}건)")


def main():
    conn = get_connection()
    init_tables(conn)

    # 과거 90일치 데이터 생성
    today = datetime.date.today()
    for i in range(90, 0, -1):
        target = today - datetime.timedelta(days=i)
        # 주말에는 신규 유저 적게, 평일에 많게
        if target.weekday() >= 5:
            num_users = random.randint(20, 40)
        else:
            num_users = random.randint(50, 80)
        generate_day_data(conn, target, num_users)

    # 오늘 데이터도 생성
    generate_day_data(conn, today, random.randint(50, 80))

    conn.close()
    print("🎉 모든 데이터 생성 완료!")


if __name__ == "__main__":
    main()
