# 🚀 가짜 구독 서비스 데이터 파이프라인

집에서 **복사-붙여넣기만으로** 따라할 수 있는 전체 데이터 엔지니어링 실습 프로젝트입니다.

## 📌 이 프로젝트로 배우는 것

| 단계 | 기술 | 실무 대응 |
|------|------|----------|
| 1 | **Docker** | 인프라 환경 구축 |
| 2 | **PostgreSQL (OLTP)** | 실시간 운영 데이터베이스 |
| 3 | **Apache Airflow** | ETL 데이터 파이프라인 스케줄링 |
| 4 | **DuckDB** | 데이터웨어하우스 (분석용 DB) |
| 5 | **PostgreSQL (Warehouse)** | BI 연결용 분석 DB |
| 6 | **Metabase** | 대시보드/리포팅 |
| 7 | **SQL** | 비즈니스 지표 분석 (MRR, Churn, ARPU) |

---

## 🏗️ 전체 아키텍처

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│  가짜 데이터    │────▶│  PostgreSQL  │────▶│   Apache Airflow │
│  생성 스크립트   │     │   (OLTP)     │     │   (ETL 파이프라인)│
└─────────────────┘     └──────────────┘     └────────┬────────┘
                                                       │
                                                       ▼
                                              ┌─────────────────┐
                                              │     DuckDB      │
                                              │  (데이터웨어하우스)│
                                              └────────┬────────┘
                                                       │
                                                       ▼
                                              ┌─────────────────┐
                                              │  PostgreSQL     │
                                              │  (Warehouse)    │
                                              └────────┬────────┘
                                                       │
                                                       ▼
                                              ┌─────────────────┐
                                              │    Metabase     │
                                              │   (대시보드)     │
                                              └─────────────────┘
```

---

## ✅ 사전 준비

- **Docker Desktop** 설치 (Windows/Mac) 또는 Linux에 Docker + Docker Compose 설치
- 약 **4GB 이상의 여유 RAM** 권장

> Docker 설치: https://docs.docker.com/get-docker/

---

## 🚀 빠른 시작 (5분)

### 1. 프로젝트 다운로드

이 폴더(`subscription-data-pipeline`)를 원하는 위치에 압축 해제합니다.

```bash
cd subscription-data-pipeline
```

### 2. 전체 인프라 띄우기

```bash
docker-compose up -d
```

> 처음 실행 시 이미지 다운로드 + Airflow 초기화에 **3~5분** 소요됩니다.

### 3. 가짜 데이터 생성 (과거 90일치)

```bash
docker exec -it oltp-db bash -c "pip install psycopg2-binary faker pandas"
docker exec -it oltp-db python3 /scripts/generate_fake_data.py
```

> ⚠️ `generate_fake_data.py`가 컨테이너 안에 없다면 아래 명령어로 복사:
> ```bash
> docker cp scripts/generate_fake_data.py oltp-db:/tmp/
> docker exec -it oltp-db python3 /tmp/generate_fake_data.py
> ```

### 4. Airflow DAG 수동 실행

브라우저에서 접속: http://localhost:8080
- ID: `admin`
- PW: `admin`

1. `subscription_pipeline` DAG 클릭
2. 왼쪽 상단 ▶️ (Trigger DAG) 버튼 클릭
3. Graph 탭에서 모든 태스크가 초록색으로 변하는지 확인

### 5. Metabase 대시보드 접속

브라우저에서 접속: http://localhost:3000
- 최초 가입: 이메일/비밀번호 아무거나 설정
- **"Add your data"** 클릭 → **PostgreSQL** 선택
- 설정값:
  - Host: `postgres-warehouse`
  - Port: `5432`
  - Database: `analytics_db`
  - Username: `warehouse_user`
  - Password: `warehouse_pass`

### 6. SQL 분석 시작

Metabase에서 **"New → SQL Query"**를 선택하고, `scripts/analytics_queries.sql`의 쿼리를 복사-붙여넣기 해보세요.

---

## 📂 프로젝트 구조

```
subscription-data-pipeline/
├── docker-compose.yml          # 전체 인프라 정의
├── dags/
│   └── subscription_pipeline.py  # Airflow ETL DAG
├── scripts/
│   ├── generate_fake_data.py   # 가짜 데이터 생성기
│   └── analytics_queries.sql   # 분석용 SQL 모음
└── data/                       # DuckDB 파일 저장소
```

---

## 🔍 DAG 상세 흐름

| 태스크 | 설명 |
|--------|------|
| `extract_yesterday` | OLTP DB에서 어제 생성된 users, subscriptions, payments 추출 |
| `transform_and_load` | DuckDB에 raw 데이터 적재 + dim/fact 테이블 재구성 |
| `compute_metrics` | MRR, Churn Rate, 국가별 매출, 코호트 등 mart 테이블 생성 |
| `sync_to_postgres` | DuckDB mart 테이블을 PostgreSQL 웨어하우스로 동기화 (Metabase 연결용) |

---

## 📊 만들 수 있는 대시보드 예시

### 1. MRR 추이 (월별 반복 매출)
```sql
SELECT 
    DATE_TRUNC('month', date) AS month,
    SUM(daily_revenue) AS mrr
FROM mart_mrr_daily
GROUP BY 1
ORDER BY 1 DESC;
```

### 2. Churn Rate 추이
```sql
SELECT 
    month,
    ROUND(SUM(churned_subscriptions) * 100.0 / SUM(new_subscriptions), 2) AS churn_rate
FROM mart_churn_monthly
GROUP BY month
ORDER BY month DESC;
```

### 3. 마케팅 채널별 유저 획득
```sql
SELECT 
    marketing_channel,
    SUM(total_users) AS users,
    SUM(d30_active) AS d30_retained
FROM mart_user_cohort
GROUP BY 1
ORDER BY 2 DESC;
```

---

## 🛠️ 트러블슈팅

### Q. `docker-compose up`이 안 돼요
```bash
# Docker Compose 버전 확인
docker-compose --version
# 또는
docker compose version

# 메모리 부족 시 일부 서비스만 띄우기
docker-compose up -d postgres-oltp postgres-warehouse metabase
```

### Q. Airflow 초기화가 끝나지 않아요
```bash
# 로그 확인
docker logs -f airflow-init

# 재시도
docker-compose down -v
docker-compose up -d
```

### Q. 가짜 데이터 생성 스크립트가 컨테이너 안에 없어요
```bash
# 직접 복사
docker cp scripts/generate_fake_data.py oltp-db:/tmp/generate.py
docker exec -it oltp-db bash
pip install psycopg2-binary faker pandas
python3 /tmp/generate.py
```

### Q. Metabase에서 DB 연결이 안 돼요
- `postgres-warehouse` 호스트명이 아닌, Docker 네트워크 내부 IP를 확인:
  ```bash
  docker network ls
  docker network inspect subscription-data-pipeline_default
  ```
- Metabase 컨테이너가 `postgres-warehouse`와 같은 네트워크에 있는지 확인

---

## 🎯 다음 단계 (확장 아이디어)

| 단계 | 도전 과제 |
|------|----------|
| **중급** | Airflow DAG에 데이터 품질 체크 추가 (Great Expectations) |
| **중급** | dbt를 도입해서 SQL 모델링 체계화 |
| **고급** | Kafka로 실시간 스트리밍 파이프라인 추가 |
| **고급** | Spark로 대용량 데이터 처리 (100GB+) |
| **실무** | AWS/GCP에 동일 구조를 클라우드로 이관 |

---

## 📚 참고 자료

- [Apache Airflow 공식 문서](https://airflow.apache.org/docs/)
- [DuckDB 공식 문서](https://duckdb.org/docs/)
- [Metabase 학습 가이드](https://www.metabase.com/learn/)
- [데이터 엔지니어링 Zoomcamp (무료)](https://github.com/DataTalksClub/data-engineering-zoomcamp)

---

**이 프로젝트는 재무팀 출신이 데이터 엔지니어링으로 전환하는 데 필요한 전체 사이클을 경험할 수 있도록 설계되었습니다.**
