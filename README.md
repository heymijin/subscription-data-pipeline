# 구독 서비스 데이터 파이프라인 (Subscription Data Pipeline)

가짜 SaaS 구독 서비스의 데이터를 **운영 DB → 데이터 파이프라인 → 데이터웨어하우스 → 대시보드**까지 흐르게 만든 엔드투엔드 데이터 엔지니어링 프로젝트입니다. Docker 위에서 PostgreSQL · Apache Airflow · Metabase를 띄워, 실제 회사의 데이터 인프라를 로컬에서 재현했습니다.

## 왜 만들었나

이전 회사에서 SAP 같은 시스템의 데이터를 **KNIME으로 ETL하고 Power BI로 대시보드**를 만들면서, 회사의 데이터 시스템이 실제로 어떻게 구성되고 연결되는지 궁금해졌다. 특히 빠르게 성장하는 산업 — 그 중심에 있는 테크 업계의 데이터 시스템을 직접 경험해보고 싶었다. 데이터가 어떻게 **저장되고, 가공되어, 실무자가 분석할 수 있는 테이블로 만들어지는지**를 내 손으로 이해하기 위해 전체 파이프라인을 구현했다.

---

## 아키텍처

```
generate_fake_data.py                 ← 가짜 데이터 생성 (Faker)
        │
        ▼
┌──────────────────────┐
│  PostgreSQL (OLTP)   │  운영 DB · subscription_db
│  users / subscriptions / payments
└──────────┬───────────┘
           │   ┌─────────────  Apache Airflow DAG  ─────────────┐
           ▼   │  extract_load → transform_and_load →           │
┌──────────────────────┐  compute_metrics → sync_check          │
│ PostgreSQL (Warehouse)│  분석 DB · analytics_db                │
│  raw_*  →  dim_* / fact_*  →  mart_*                           │
└──────────┬───────────┘                                        │
           │   └────────────────────────────────────────────────┘
           ▼
┌──────────────────────┐
│      Metabase        │  SQL 조회 + 대시보드
└──────────────────────┘
```

## 기술 스택

| 계층 | 도구 | 역할 |
|------|------|------|
| 인프라 | **Docker Compose** | 모든 서비스를 한 번에 실행 |
| 운영 DB (OLTP) | **PostgreSQL** | 서비스 데이터가 실시간으로 쌓이는 원천 |
| 파이프라인 | **Apache Airflow** | ETL 자동화 및 스케줄링 (DAG) |
| 데이터웨어하우스 | **PostgreSQL** | 분석용으로 정리한 raw / dim·fact / mart |
| BI | **Metabase** | SQL 분석 및 대시보드 |
| 분석 | **SQL** | MRR, Churn, ARPU 등 비즈니스 지표 |

## 데이터 모델 (별 스키마)

- **raw_** : OLTP에서 그대로 복사해온 원본 (`raw_users`, `raw_subscriptions`, `raw_payments`)
- **dim_ / fact_** : 분석하기 좋게 가공한 차원·사실 테이블 (`dim_users`, `dim_plans`, `fact_subscriptions`, `fact_payments`)
- **mart_** : 질문별로 미리 집계한 최종 표 (`mart_mrr_daily`, `mart_churn_monthly`, `mart_revenue_by_country`, `mart_user_cohort`)

## Airflow DAG (`subscription_pipeline`)

| 태스크 | 하는 일 |
|--------|---------|
| `extract_load` | OLTP의 users/subscriptions/payments를 웨어하우스 `raw_*`로 복사 (pandas `to_sql`, 날짜 타입 보존) |
| `transform_and_load` | `raw_*`를 SQL로 가공해 `dim_*` / `fact_*` 생성 |
| `compute_metrics` | `dim`·`fact`를 집계해 `mart_*` (MRR·Churn·국가별 매출·코호트) 생성 |
| `sync_check` | 결과 행 수를 확인해 파이프라인 정상 여부 검증 |

> 태스크끼리 데이터를 넘기지 않고 각 단계가 DB에서 직접 읽고 쓰는 구조로, XCom을 통한 대용량 데이터 전달을 피했습니다.

---

## 실행 방법

**사전 준비:** Docker Desktop, 여유 RAM 4GB+

```bash
# 1. 인프라 전체 실행 (처음엔 이미지 다운로드로 3~5분)
docker compose up -d

# 2. 가짜 데이터 생성 (과거 90일치를 OLTP에 적재)
pip install psycopg2-binary faker pandas
DB_HOST=localhost python3 scripts/generate_fake_data.py

# 3. Airflow에서 파이프라인 실행
#    http://localhost:8080  (admin / admin)
#    subscription_pipeline DAG → Trigger → 4개 태스크 초록불 확인

# 4. Metabase에서 대시보드
#    http://localhost:3000  → PostgreSQL 연결:
#    host: postgres-warehouse · db: analytics_db
#    user: warehouse_user · pw: warehouse_pass
```

> 💡 `docker-compose.yml`의 비밀번호는 **로컬 학습용 임시 크레덴셜**입니다. 실제 운영에서는 환경변수/시크릿으로 분리해야 합니다.
> ⚠️ 데이터를 초기화하려면 `docker compose down`(데이터 유지) 사용. `-v` 옵션은 볼륨(데이터)까지 삭제하니 주의하세요.

## 프로젝트 구조

```
subscription-data-pipeline/
├── docker-compose.yml            # 전체 인프라 정의 (OLTP·Warehouse·Airflow·Metabase)
├── dags/
│   └── subscription_pipeline.py  # Airflow ETL DAG (4 tasks)
├── scripts/
│   ├── generate_fake_data.py     # 가짜 데이터 생성기
│   └── analytics_queries.sql     # 분석용 SQL 모음
└── .gitignore
```

---

## 내가 해결한 문제들 (트러블슈팅)

파이프라인을 돌리며 실제로 막혔고, 로그를 읽어 원인을 찾아 해결한 기록입니다.

> Airflow DAG가 계속 실패(빨간불)해서 처음엔 코드를 여기저기 고쳐봤지만 해결되지 않았다. 로그를 다시 읽으며 근본 원인을 추적한 결과, **태스크 간에 데이터를 JSON으로 주고받는 과정에서 날짜가 숫자로 변환돼** `DATE` 컬럼에 들어가지 못하는 게 문제였다. 데이터 전달 구조 자체를 바꾸자 모든 태스크가 성공(파란불)했고, 웨어하우스와 Metabase까지 데이터가 정상 적재되었다.

### 1. 날짜가 숫자로 깨져 적재 실패 (`BIGINT → DATE` 캐스팅 에러)
- **원인:** 태스크 간 데이터를 XCom + `to_json()`으로 넘기는 과정에서 날짜가 밀리초 정수로 변환되어 `DATE` 컬럼에 들어가지 못함.
- **해결:** XCom 전달 방식을 제거하고 각 태스크가 DB에서 직접 읽고 쓰도록 리팩터링. (부가 효과로 대용량 XCom 안티패턴도 제거)

### 2. 만든 테이블이 저장되지 않음
- **원인:** SQLAlchemy `.connect()`는 변경사항을 자동 커밋하지 않아, 생성한 테이블이 다음 태스크에서 조회되지 않음.
- **해결:** `wh_engine.begin()`으로 트랜잭션을 열어 블록 종료 시 자동 커밋되도록 변경.

### 3. `column "plan_type" does not exist`
- **원인:** 매출 마트(`mart_mrr_daily`)를 만들 때 결제 테이블(`fact_payments`)로 요금제별 집계를 시도했으나, 요금제 정보는 구독 테이블에만 존재.
- **해결:** `fact_payments`와 `fact_subscriptions`를 `subscription_id`로 JOIN해 요금제를 연결.

---

## 이 프로젝트로 배운 것

- 여러 프로그램을 로컬에 일일이 설치하면 저장공간과 시스템 자원을 계속 차지하는데, **Docker로 컨테이너화**하면 필요할 때만 함께 띄우고(`up`) 내릴(`down`) 수 있고 어디서든 동일한 환경을 재현할 수 있다는 것을 배웠다.
- 실시간으로 쌓이는 데이터가 **PostgreSQL 같은 DBMS(운영 DB)**에 저장되는 구조를 이해했다.
- **Airflow** 같은 데이터 파이프라인이 그 원본을 가공해, raw 테이블뿐 아니라 정리된 테이블(dim·fact)까지 **데이터웨어하우스**에 적재하는 흐름을 배웠다.
- 그 데이터를 분석하기 쉽게 한 번 더 집계한 것이 **데이터 마트**이며, 질문별 최종 표가 여기 저장된다는 걸 알게 됐다.
- 실무자는 **Metabase 같은 BI 툴에서 SQL로** 원하는 데이터를 뽑아 사업 분석용 대시보드를 만든다는 전체 사이클을 경험했다.
- 파이프라인이 안 돌 때 **로그를 읽어 원인을 찾고 고치는 것**이 데이터 엔지니어링의 큰 부분임을 체감했다.

## 다음 단계

- [ ] 해지율·국가별 매출 차트를 추가해 대시보드 확장
- [ ] dbt 도입으로 SQL 모델링 체계화
- [ ] 데이터 품질 체크(Great Expectations) 추가
- [ ] AWS/GCP로 클라우드 이관
