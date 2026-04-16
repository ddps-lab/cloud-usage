-- =============================================================================
-- monitor_v2/cost/queries.sql
--
-- Cost Explorer API 호출 → Athena CUR 쿼리 매핑
--
-- 대상 테이블: hyu_ddps_logs.cur_logs
-- 파티션:      year (STRING), month (STRING, zero-padded)
--
-- CE API ↔ CUR 컬럼 대응
--   SERVICE dimension    → product_product_name
--   REGION dimension     → product_region_code  (빈 값 = global 서비스)
--   aws:createdBy TAG    → resource_tags_aws_created_by
--   UnblendedCost metric → line_item_unblended_cost
--   일별 날짜 필터       → DATE(line_item_usage_start_date)
--
-- 날짜 파라미터 (실행 전 치환)
--   {d1_date}    리포트 기준일      예: '2026-04-04'  (today - 2일, CE 지연 보정)
--   {d2_date}    전일 비교일        예: '2026-04-03'  (today - 3일)
--   {mtd_start}  MTD 시작일        예: '2026-04-01'  (d1_date 월의 1일)
--   {year}       파티션 연도        예: '2026'
--   {month}      파티션 월(두자리)  예: '04'
--
-- NOTE: line_item_line_item_type 미필터 → CE API 기본 동작(전체 유형 합산)과 동일.
--       크레딧·세금 제외가 필요하면 WHERE 절에 아래를 추가:
--       AND line_item_line_item_type NOT IN ('Credit', 'Refund', 'EdpDiscount')
--
-- Q9~Q11: Main 3 비용 변화 AI 분석용 (analysis.py 참조)
--   d1_date vs d2_date 증감을 서비스 → 리소스 타입 → 리소스 ID 순으로 드릴다운
--   ABS(diff) DESC 정렬: 증가/감소 모두 포함, 변동 큰 항목 우선
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Q1. fetch_daily_by_service (D-1)
--     CE: get_cost_and_usage(GroupBy=[SERVICE], Granularity=DAILY, period=d1)
--     data.py: fetch_daily_by_service(ce, period_d1)  → daily_d1
--     결과:  {service: float}
-- -----------------------------------------------------------------------------
SELECT
    product_product_name                    AS service,
    SUM(line_item_unblended_cost)           AS cost
FROM hyu_ddps_logs.cur_logs
WHERE year  = '2026'
  AND month = '4'
  AND DATE(line_item_usage_start_date) = DATE('2026-04-05')
GROUP BY product_product_name
HAVING SUM(line_item_unblended_cost) > 0.01
ORDER BY cost DESC;

-- -----------------------------------------------------------------------------
-- Q2. fetch_daily_by_service (D-2)
--     CE: get_cost_and_usage(GroupBy=[SERVICE], Granularity=DAILY, period=d2)
--     data.py: fetch_daily_by_service(ce, period_d2)  → daily_d2
--     결과:  {service: float}
--     ※ Q1과 동일 구조, 날짜만 {d2_date}로 교체
-- -----------------------------------------------------------------------------
SELECT
    product_product_name                    AS service,
    SUM(line_item_unblended_cost)           AS cost
FROM hyu_ddps_logs.cur_logs
WHERE year  = '2026'
  AND month = '4'
  AND DATE(line_item_usage_start_date) = DATE('2026-04-04')
GROUP BY product_product_name
HAVING SUM(line_item_unblended_cost) > 0.01
ORDER BY cost DESC;


-- -----------------------------------------------------------------------------
-- Q3. fetch_daily_by_service_and_creator (D-1)
--     CE: get_cost_and_usage(GroupBy=[SERVICE, TAG(aws:createdBy)], Granularity=DAILY)
--     data.py: fetch_daily_by_service_and_creator(ce, period_d1)  → by_creator
--     결과:  {service: {creator_label: float}}
--
--     Creator 분류 우선순위 (Tax 제외):
--     1. 공통 서비스 → [공통] prefix
--     2. aws:createdBy 태그 → username (SPLIT_PART로 추출)
--     3. custom 태그들 (username, requester, project, ...) → [prefix] value
--     4. Usage type 있음 → {service} - {usage_type}
--     5. 그 외 → {service} - 기타
-- ※ Tax 항목은 IAM User별 집계 대상이 아니므로 제외 (WHERE line_item_line_item_type != 'Tax')
-- -----------------------------------------------------------------------------
SELECT
    product_product_name                                            AS service,
    CASE
        WHEN product_product_name = 'AWS Data Transfer'
            THEN '[공통] Data Transfer'
        WHEN product_product_name = 'AWS Cost Explorer'
            THEN '[공통] Cost Explorer'
        WHEN product_product_name = 'AWS Support [Business]'
            THEN '[공통] Support'
        WHEN NULLIF(resource_tags_aws_created_by, '') IS NOT NULL
            THEN SPLIT_PART(resource_tags_aws_created_by, ':', 3)
        WHEN NULLIF(resource_tags_user_name, '') IS NOT NULL
            THEN resource_tags_user_name
        WHEN line_item_line_item_type = 'Usage'
            THEN CONCAT(product_product_name, ' - ', line_item_usage_type)
        ELSE CONCAT(product_product_name, ' - 기타')
    END                                                             AS creator,
    SUM(line_item_unblended_cost)                                   AS cost
FROM hyu_ddps_logs.cur_logs
WHERE year  = '{year}'
  AND month = '{month}'
  AND DATE(line_item_usage_start_date) = DATE('{d1_date}')
  AND line_item_line_item_type != 'Tax'
GROUP BY
    product_product_name,
    CASE
        WHEN product_product_name = 'AWS Data Transfer'
            THEN '[공통] Data Transfer'
        WHEN product_product_name = 'AWS Cost Explorer'
            THEN '[공통] Cost Explorer'
        WHEN product_product_name = 'AWS Support [Business]'
            THEN '[공통] Support'
        WHEN NULLIF(resource_tags_aws_created_by, '') IS NOT NULL
            THEN SPLIT_PART(resource_tags_aws_created_by, ':', 3)
        WHEN NULLIF(resource_tags_user_name, '') IS NOT NULL
            THEN resource_tags_user_name
        WHEN line_item_line_item_type = 'Usage'
            THEN CONCAT(product_product_name, ' - ', line_item_usage_type)
        ELSE CONCAT(product_product_name, ' - 기타')
    END
HAVING SUM(line_item_unblended_cost) > 0.01
ORDER BY service, cost DESC;


-- -----------------------------------------------------------------------------
-- Q4. fetch_daily_by_service_and_region (D-1)
--     CE: get_cost_and_usage(GroupBy=[SERVICE, REGION], Granularity=DAILY)
--     data.py: fetch_daily_by_service_and_region(ce, period_d1)  → by_region
--     결과:  {service: {region: float}}
--
--     빈 region_code = 글로벌 서비스(Route53, IAM 등) → 'global'
-- -----------------------------------------------------------------------------
SELECT
    product_product_name                                            AS service,
    COALESCE(
        NULLIF(resource_tags_aws_created_by, ''),
        'aws:createdBy 태그 없음'
    )                                                               AS creator,
    SUM(line_item_unblended_cost)                                   AS cost
FROM hyu_ddps_logs.cur_logs
WHERE year  = '2026'
  AND month = '4'
  AND DATE(line_item_usage_start_date) = DATE('2026-04-05')
GROUP BY
    product_product_name,
    COALESCE(NULLIF(resource_tags_aws_created_by, ''), 'aws:createdBy 태그 없음')
HAVING SUM(line_item_unblended_cost) > 0.01
ORDER BY cost, service DESC;



-- -----------------------------------------------------------------------------
-- Q5. fetch_mtd_by_service_and_creator
--     CE: get_cost_and_usage(GroupBy=[SERVICE, TAG(aws:createdBy)], Granularity=MONTHLY)
--     data.py: fetch_mtd_by_service_and_creator(ce, period_mtd_this)  → by_creator_mtd
--     결과:  {service: {creator_label: float}}
--
--     MTD 범위: {mtd_start} ~ {d1_date} (inclusive)
--     당월 1일에 실행된 경우 범위가 비므로 Python에서 {} 반환 (이 쿼리 미실행)
--
--     Creator 분류는 Q3과 동일한 우선순위 적용 (Tax 제외)
-- ※ Tax 항목은 IAM User별 집계 대상이 아니므로 제외
-- -----------------------------------------------------------------------------
SELECT
    product_product_name                                            AS service,
    CASE
        WHEN product_product_name = 'AWS Data Transfer'
            THEN '[공통] Data Transfer'
        WHEN product_product_name = 'AWS Cost Explorer'
            THEN '[공통] Cost Explorer'
        WHEN product_product_name = 'AWS Support [Business]'
            THEN '[공통] Support'
        WHEN NULLIF(resource_tags_aws_created_by, '') IS NOT NULL
            THEN SPLIT_PART(resource_tags_aws_created_by, ':', 3)
        WHEN NULLIF(resource_tags_user_username, '') IS NOT NULL
            THEN CONCAT('[username] ', resource_tags_user_username)
        WHEN NULLIF(resource_tags_user_requester, '') IS NOT NULL
            THEN CONCAT('[requester] ', resource_tags_user_requester)
        WHEN NULLIF(resource_tags_user_project, '') IS NOT NULL
            THEN CONCAT('[project] ', resource_tags_user_project)
        WHEN NULLIF(resource_tags_user_project_name, '') IS NOT NULL
            THEN CONCAT('[project_name] ', resource_tags_user_project_name)
        WHEN NULLIF(resource_tags_user_name, '') IS NOT NULL
            THEN resource_tags_user_name
        WHEN NULLIF(resource_tags_user_n_a_m_e, '') IS NOT NULL
            THEN CONCAT('[n_a_m_e] ', resource_tags_user_n_a_m_e)
        WHEN NULLIF(resource_tags_user_environment, '') IS NOT NULL
            THEN CONCAT('[environment] ', resource_tags_user_environment)
        WHEN line_item_line_item_type = 'Usage'
            THEN CONCAT(product_product_name, ' - ', line_item_usage_type)
        ELSE CONCAT(product_product_name, ' - 기타')
    END                                                             AS creator,
    SUM(line_item_unblended_cost)                                   AS cost
FROM hyu_ddps_logs.cur_logs
WHERE year  = '{year}'
  AND month = '{month}'
  AND DATE(line_item_usage_start_date) BETWEEN DATE('{mtd_start}') AND DATE('{d1_date}')
  AND line_item_line_item_type != 'Tax'
GROUP BY
    product_product_name,
    CASE
        WHEN product_product_name = 'AWS Data Transfer'
            THEN '[공통] Data Transfer'
        WHEN product_product_name = 'AWS Cost Explorer'
            THEN '[공통] Cost Explorer'
        WHEN product_product_name = 'AWS Support [Business]'
            THEN '[공통] Support'
        WHEN NULLIF(resource_tags_aws_created_by, '') IS NOT NULL
            THEN SPLIT_PART(resource_tags_aws_created_by, ':', 3)
        WHEN NULLIF(resource_tags_user_username, '') IS NOT NULL
            THEN CONCAT('[username] ', resource_tags_user_username)
        WHEN NULLIF(resource_tags_user_requester, '') IS NOT NULL
            THEN CONCAT('[requester] ', resource_tags_user_requester)
        WHEN NULLIF(resource_tags_user_project, '') IS NOT NULL
            THEN CONCAT('[project] ', resource_tags_user_project)
        WHEN NULLIF(resource_tags_user_project_name, '') IS NOT NULL
            THEN CONCAT('[project_name] ', resource_tags_user_project_name)
        WHEN NULLIF(resource_tags_user_name, '') IS NOT NULL
            THEN resource_tags_user_name
        WHEN NULLIF(resource_tags_user_n_a_m_e, '') IS NOT NULL
            THEN CONCAT('[n_a_m_e] ', resource_tags_user_n_a_m_e)
        WHEN NULLIF(resource_tags_user_environment, '') IS NOT NULL
            THEN CONCAT('[environment] ', resource_tags_user_environment)
        WHEN line_item_line_item_type = 'Usage'
            THEN CONCAT(product_product_name, ' - ', line_item_usage_type)
        ELSE CONCAT(product_product_name, ' - 기타')
    END
HAVING SUM(line_item_unblended_cost) > 0.01
ORDER BY service, cost DESC;


-- -----------------------------------------------------------------------------
-- Q6. fetch_mtd_by_service_and_region
--     CE: get_cost_and_usage(GroupBy=[SERVICE, REGION], Granularity=MONTHLY)
--     data.py: fetch_mtd_by_service_and_region(ce, period_mtd_this)  → by_region_mtd
--     결과:  {service: {region: float}}
-- -----------------------------------------------------------------------------
SELECT
    product_product_name                                AS service,
    COALESCE(NULLIF(product_region_code, ''), 'global') AS region,
    SUM(line_item_unblended_cost)                       AS cost
FROM hyu_ddps_logs.cur_logs
WHERE year  = '2026'
  AND month = '4'
  AND DATE(line_item_usage_start_date) BETWEEN DATE('2026-04-01') AND DATE('2026-04-05')
GROUP BY
    product_product_name,
    COALESCE(NULLIF(product_region_code, ''), 'global')
HAVING SUM(line_item_unblended_cost) > 0.01
ORDER BY cost DESC;


-- -----------------------------------------------------------------------------
-- Q7. fetch_mtd_total
--     CE: get_cost_and_usage(GroupBy=없음, Granularity=MONTHLY)
--     data.py: fetch_mtd_total(ce, period_mtd_this)  → mtd_this (float)
--     결과:  단일 합계 float
-- -----------------------------------------------------------------------------
SELECT
    SUM(line_item_unblended_cost)   AS mtd_total
FROM hyu_ddps_logs.cur_logs
WHERE year  = '2026'
  AND month = '4'
  AND DATE(line_item_usage_start_date) BETWEEN DATE('2026-04-01') AND DATE('2026-04-05');

-- -----------------------------------------------------------------------------
-- Q8. fetch_cost_forecast
--     CE: get_cost_forecast(Granularity=MONTHLY, Metric=UNBLENDED_COST)
--     → CUR는 과거 데이터만 저장하므로 SQL로 직접 예측 불가.
--        아래는 참고용 선형 추세 추정 쿼리 (정확도 낮음, 검증 목적).
--
--     원리: 당월 경과일 비용 ÷ 경과일 수 × 잔여일 수 = 잔여 예상 비용
--           projected = mtd_actual + daily_avg * days_remaining
-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
-- Q9. fetch_service_diff (analysis.py Main 3)
--     서비스별 d1 vs d2 비용 차이. 변동 절대값 큰 순 TOP 10.
--     증가(diff > 0) / 감소(diff < 0) 모두 포함.
-- -----------------------------------------------------------------------------
SELECT
    product_product_name AS service,
    SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-08')
             THEN line_item_unblended_cost ELSE 0 END) AS cost_d1,
    SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-07')
             THEN line_item_unblended_cost ELSE 0 END) AS cost_d2,
    SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-08')
             THEN line_item_unblended_cost ELSE 0 END)
  - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-07')
             THEN line_item_unblended_cost ELSE 0 END) AS diff
FROM hyu_ddps_logs.cur_logs
WHERE year = '2026'
  AND month = '4'
  AND DATE(line_item_usage_start_date) IN (DATE('2026-04-08'), DATE('2026-04-07'))
GROUP BY product_product_name
HAVING ABS(
    SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-08')
             THEN line_item_unblended_cost ELSE 0 END)
  - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-07')
             THEN line_item_unblended_cost ELSE 0 END)
) > 0.01
ORDER BY ABS(diff) DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- Q10. fetch_usage_type_diff (analysis.py Main 3)
--      리소스 타입별 d1 vs d2 비용 차이. (line_item_usage_type)
--      예: BoxUsage:t3.medium, USW2-EBS:VolumeUsage.gp3
-- -----------------------------------------------------------------------------
  SELECT
      product_product_name AS service,
      line_item_usage_type AS usage_type,
      SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-08')
               THEN line_item_unblended_cost ELSE 0 END) AS cost_d1,
      SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-07')
               THEN line_item_unblended_cost ELSE 0 END) AS cost_d2,
      SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-08')
               THEN line_item_unblended_cost ELSE 0 END)
    - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-07')
               THEN line_item_unblended_cost ELSE 0 END) AS diff
  FROM hyu_ddps_logs.cur_logs
  WHERE year  = '2026'
    AND month IN ('4', '4')
    AND DATE(line_item_usage_start_date) IN (DATE('2026-04-08'), DATE('2026-04-07'))
  GROUP BY product_product_name, line_item_usage_type
  HAVING ABS(
      SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-08')
               THEN line_item_unblended_cost ELSE 0 END)
    - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-07')
               THEN line_item_unblended_cost ELSE 0 END)
  ) > 0.01
  ORDER BY ABS(
      SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-08')
               THEN line_item_unblended_cost ELSE 0 END)
    - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-07')
               THEN line_item_unblended_cost ELSE 0 END)
  ) DESC
  LIMIT 10;


-- -----------------------------------------------------------------------------
-- Q11. fetch_resource_diff (analysis.py Main 3)
--      리소스 ID별 d1 vs d2 비용 차이. (line_item_resource_id)
--      예: i-005217980755bcf43, vol-xxxx, arn:aws:s3:::bucket-name
--      resource_id 빈값 제외.
-- -----------------------------------------------------------------------------
SELECT
      product_product_name  AS service,
      line_item_usage_type  AS usage_type,
      line_item_resource_id AS resource_id,
      SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-08')
               THEN line_item_unblended_cost ELSE 0 END) AS cost_d1,
      SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-07')
               THEN line_item_unblended_cost ELSE 0 END) AS cost_d2,
      SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-08')
               THEN line_item_unblended_cost ELSE 0 END)
    - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-07')
               THEN line_item_unblended_cost ELSE 0 END) AS diff
  FROM hyu_ddps_logs.cur_logs
  WHERE year  = '2026'
    AND month IN ('4', '4')
    AND DATE(line_item_usage_start_date) IN (DATE('2026-04-08'), DATE('2026-04-07'))
    AND line_item_resource_id IS NOT NULL
    AND line_item_resource_id != ''
  GROUP BY product_product_name, line_item_usage_type, line_item_resource_id
  HAVING ABS(
      SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-08')
               THEN line_item_unblended_cost ELSE 0 END)
    - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-07')
               THEN line_item_unblended_cost ELSE 0 END)
  ) > 0.01
  ORDER BY ABS(
      SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-08')
               THEN line_item_unblended_cost ELSE 0 END)
    - SUM(CASE WHEN DATE(line_item_usage_start_date) = DATE('2026-04-07')
               THEN line_item_unblended_cost ELSE 0 END)
  ) DESC
  LIMIT 10;


-- -----------------------------------------------------------------------------
-- Q12. collect_instance_cost_cur (ec2/data_cur.py)
--      인스턴스 ID별 D-1 On-Demand EC2 비용 + 실 사용 시간 (CUR 기반)
--      Spot 전환 절감 분석용. BoxUsage 행만 대상 (On-Demand 인스턴스만 포함).
--
--      iam_user: resource_tags_aws_created_by → SPLIT_PART(..., ':', 3)
--                예: 'IAMUser:AIDAXXX:alice' → 'alice'
--      usage_hours: line_item_usage_amount 합산 (시간 단위)
--      cost:        line_item_unblended_cost 합산 (USD)
--
--      spot_estimate = usage_hours × describe_spot_price_history 결과
--      savings       = cost - spot_estimate
-- -----------------------------------------------------------------------------
SELECT
    line_item_resource_id                                               AS instance_id,
    product_instance_type                                               AS instance_type,
    COALESCE(NULLIF(product_region_code, ''), 'global')                AS region,
    SPLIT_PART(COALESCE(resource_tags_aws_created_by, ''), ':', 3)     AS iam_user,
    SUM(line_item_usage_amount)                                         AS usage_hours,
    SUM(line_item_unblended_cost)                                       AS cost
FROM hyu_ddps_logs.cur_logs
WHERE year  = '{year}'
  AND month = '{month}'
  AND DATE(line_item_usage_start_date) = DATE('{d1_date}')
  AND line_item_resource_id LIKE 'i-%'
  AND product_instance_type != ''
  AND line_item_usage_type  LIKE '%BoxUsage%'
GROUP BY
    line_item_resource_id,
    product_instance_type,
    COALESCE(NULLIF(product_region_code, ''), 'global'),
    SPLIT_PART(COALESCE(resource_tags_aws_created_by, ''), ':', 3)
HAVING SUM(line_item_unblended_cost) > 0;


-- -----------------------------------------------------------------------------
-- Q13. fetch_weekend_ec2_by_week (분석용 — query.py)
--      EC2 + EC2-Other 서비스의 주말(토·일) 비용을 주 단위로 집계.
--
--      대상 기간: 2026년 3월 ~ 4월
--      대상 요일: day_of_week=6(토), day_of_week=7(일)  ← Presto/Athena 기준
--                 (1=월 … 6=토 … 7=일)
--      EC2 서비스:
--        - 'Amazon Elastic Compute Cloud - Compute'  (인스턴스 계산 비용)
--        - 'Amazon Elastic Compute Cloud'
--        - 'Amazon EC2'
--        - 'EC2 - Other'                             (EBS·NAT·전송 등)
--
--      출력 컬럼:
--        week_start   해당 주 월요일 (date_trunc 기준)
--        saturday     해당 주 토요일 (week_start + 5일)
--        sunday       해당 주 일요일 (week_start + 6일)
--        ec2_sat      EC2 Compute 토요일 비용
--        ec2_sun      EC2 Compute 일요일 비용
--        ec2_weekend  EC2 Compute 주말 합계
--        other_sat    EC2-Other 토요일 비용
--        other_sun    EC2-Other 일요일 비용
--        other_weekend EC2-Other 주말 합계
--        weekend_total EC2 전체 주말 합계
-- -----------------------------------------------------------------------------
SELECT
    DATE(date_trunc('week', DATE(line_item_usage_start_date)))
                                                                        AS week_start,
    DATE(date_trunc('week', DATE(line_item_usage_start_date))) + INTERVAL '5' DAY
                                                                        AS saturday,
    DATE(date_trunc('week', DATE(line_item_usage_start_date))) + INTERVAL '6' DAY
                                                                        AS sunday,
    SUM(CASE
        WHEN product_product_name IN (
            'Amazon Elastic Compute Cloud - Compute',
            'Amazon Elastic Compute Cloud',
            'Amazon EC2'
        ) AND day_of_week(DATE(line_item_usage_start_date)) = 6
        THEN line_item_unblended_cost ELSE 0
    END)                                                                AS ec2_sat,
    SUM(CASE
        WHEN product_product_name IN (
            'Amazon Elastic Compute Cloud - Compute',
            'Amazon Elastic Compute Cloud',
            'Amazon EC2'
        ) AND day_of_week(DATE(line_item_usage_start_date)) = 7
        THEN line_item_unblended_cost ELSE 0
    END)                                                                AS ec2_sun,
    SUM(CASE
        WHEN product_product_name IN (
            'Amazon Elastic Compute Cloud - Compute',
            'Amazon Elastic Compute Cloud',
            'Amazon EC2'
        )
        THEN line_item_unblended_cost ELSE 0
    END)                                                                AS ec2_weekend,
    SUM(CASE
        WHEN product_product_name = 'EC2 - Other'
         AND day_of_week(DATE(line_item_usage_start_date)) = 6
        THEN line_item_unblended_cost ELSE 0
    END)                                                                AS other_sat,
    SUM(CASE
        WHEN product_product_name = 'EC2 - Other'
         AND day_of_week(DATE(line_item_usage_start_date)) = 7
        THEN line_item_unblended_cost ELSE 0
    END)                                                                AS other_sun,
    SUM(CASE
        WHEN product_product_name = 'EC2 - Other'
        THEN line_item_unblended_cost ELSE 0
    END)                                                                AS other_weekend,
    SUM(line_item_unblended_cost)                                       AS weekend_total
FROM hyu_ddps_logs.cur_logs
WHERE year  = '2026'
  AND month IN ('3', '4')
  AND product_product_name IN (
      'Amazon Elastic Compute Cloud - Compute',
      'Amazon Elastic Compute Cloud',
      'Amazon EC2',
      'EC2 - Other'
  )
  AND day_of_week(DATE(line_item_usage_start_date)) IN (6, 7)
GROUP BY date_trunc('week', DATE(line_item_usage_start_date))
HAVING SUM(line_item_unblended_cost) > 0
ORDER BY week_start;


-- -----------------------------------------------------------------------------
-- Q14. fetch_weekday_ec2_by_week (분석용)
--      EC2 + EC2-Other 서비스의 평일(월~금) 비용을 주 단위로 집계하고
--      LAG 윈도우 함수로 전주 대비 변화를 함께 표시한다.
--
--      대상 기간: 2026년 3월 ~ 4월
--      대상 요일: day_of_week IN (1,2,3,4,5)  ← 월=1 … 금=5  (Presto/Athena 기준)
--
--      집계 컬럼:
--        week_start      해당 주 월요일
--        friday          해당 주 금요일 (week_start + 4일)
--        ec2_weekday     EC2 Compute 평일 합계
--        other_weekday   EC2-Other 평일 합계
--        weekday_total   EC2 전체 평일 합계
--
--      전주 비교 컬럼 (LAG):
--        prev_ec2_weekday    전주 EC2 Compute 평일 합계
--        prev_other_weekday  전주 EC2-Other 평일 합계
--        prev_weekday_total  전주 전체 평일 합계
--        diff_ec2            ec2_weekday   - prev_ec2_weekday   (증가 +, 감소 -)
--        diff_other          other_weekday - prev_other_weekday
--        diff_total          weekday_total - prev_weekday_total
--        pct_total           diff_total / prev_weekday_total × 100 (NULL = 첫 주)
-- -----------------------------------------------------------------------------
WITH weekday_base AS (
    SELECT
        DATE(date_trunc('week', DATE(line_item_usage_start_date)))
                                                                        AS week_start,
        DATE(date_trunc('week', DATE(line_item_usage_start_date))) + INTERVAL '4' DAY
                                                                        AS friday,
        SUM(CASE
            WHEN product_product_name IN (
                'Amazon Elastic Compute Cloud - Compute',
                'Amazon Elastic Compute Cloud',
                'Amazon EC2'
            )
            THEN line_item_unblended_cost ELSE 0
        END)                                                            AS ec2_weekday,
        SUM(CASE
            WHEN product_product_name = 'EC2 - Other'
            THEN line_item_unblended_cost ELSE 0
        END)                                                            AS other_weekday,
        SUM(line_item_unblended_cost)                                   AS weekday_total
    FROM hyu_ddps_logs.cur_logs
    WHERE year  = '2026'
      AND month IN ('3', '4')
      AND product_product_name IN (
          'Amazon Elastic Compute Cloud - Compute',
          'Amazon Elastic Compute Cloud',
          'Amazon EC2',
          'EC2 - Other'
      )
      AND day_of_week(DATE(line_item_usage_start_date)) IN (1, 2, 3, 4, 5)
    GROUP BY date_trunc('week', DATE(line_item_usage_start_date))
    HAVING SUM(line_item_unblended_cost) > 0
)
SELECT
    week_start,
    friday,
    ec2_weekday,
    other_weekday,
    weekday_total,
    LAG(ec2_weekday)    OVER (ORDER BY week_start)                      AS prev_ec2_weekday,
    LAG(other_weekday)  OVER (ORDER BY week_start)                      AS prev_other_weekday,
    LAG(weekday_total)  OVER (ORDER BY week_start)                      AS prev_weekday_total,
    ec2_weekday   - LAG(ec2_weekday)   OVER (ORDER BY week_start)       AS diff_ec2,
    other_weekday - LAG(other_weekday) OVER (ORDER BY week_start)       AS diff_other,
    weekday_total - LAG(weekday_total) OVER (ORDER BY week_start)       AS diff_total,
    CASE
        WHEN LAG(weekday_total) OVER (ORDER BY week_start) IS NOT NULL
         AND LAG(weekday_total) OVER (ORDER BY week_start) > 0
        THEN ROUND(
            (weekday_total - LAG(weekday_total) OVER (ORDER BY week_start))
            / LAG(weekday_total) OVER (ORDER BY week_start) * 100,
            1
        )
        ELSE NULL
    END                                                                 AS pct_total
FROM weekday_base
ORDER BY week_start;


-- -----------------------------------------------------------------------------
-- Q8. fetch_cost_forecast
SELECT
    SUM(line_item_unblended_cost)                                       AS mtd_actual,
    DATE_DIFF('day', DATE('{mtd_start}'), DATE('{d1_date}')) + 1       AS days_elapsed,
    -- 이달 말일까지 잔여일 (말일 = month의 마지막 날, Athena: LAST_DAY_OF_MONTH 미지원)
    -- 직접 치환: {days_in_month} = 해당 월 총 일수 (예: 30)
    {days_in_month} - (DATE_DIFF('day', DATE('{mtd_start}'), DATE('{d1_date}')) + 1)
                                                                        AS days_remaining,
    SUM(line_item_unblended_cost)
        / (DATE_DIFF('day', DATE('{mtd_start}'), DATE('{d1_date}')) + 1)
                                                                        AS daily_avg,
    SUM(line_item_unblended_cost)
        + SUM(line_item_unblended_cost)
            / (DATE_DIFF('day', DATE('{mtd_start}'), DATE('{d1_date}')) + 1)
            * ({days_in_month} - (DATE_DIFF('day', DATE('{mtd_start}'), DATE('{d1_date}')) + 1))
                                                                        AS projected_total
FROM hyu_ddps_logs.cur_logs
WHERE year  = '{year}'
  AND month = '{month}'
  AND DATE(line_item_usage_start_date) BETWEEN DATE('{mtd_start}') AND DATE('{d1_date}');
