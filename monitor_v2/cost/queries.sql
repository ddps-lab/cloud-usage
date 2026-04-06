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
HAVING SUM(line_item_unblended_cost) > 0
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
HAVING SUM(line_item_unblended_cost) > 0
ORDER BY cost DESC;


-- -----------------------------------------------------------------------------
-- Q3. fetch_daily_by_service_and_creator (D-1)
--     CE: get_cost_and_usage(GroupBy=[SERVICE, TAG(aws:createdBy)], Granularity=DAILY)
--     data.py: fetch_daily_by_service_and_creator(ce, period_d1)  → by_creator
--     결과:  {service: {creator_label: float}}
--
--     CE TAG 값 'aws:createdBy$IAMUser:arn:alice' → '$' 뒤가 실제 creator
--     CUR는 resource_tags_aws_created_by 에 그 값이 그대로 저장됨
--     미태깅 = NULL 또는 빈 문자열 → 'aws:createdBy 태그 없음' 처리
-- -----------------------------------------------------------------------------
SELECT
    product_product_name                                            AS service,
    COALESCE(
        NULLIF(resource_tags_aws_created_by, ''),
        'aws:createdBy 태그 없음'
    )                                                               AS creator,
    SUM(line_item_unblended_cost)                                   AS cost
FROM hyu_ddps_logs.cur_logs
WHERE year  = '{year}'
  AND month = '{month}'
  AND DATE(line_item_usage_start_date) = DATE('{d1_date}')
GROUP BY
    product_product_name,
    COALESCE(NULLIF(resource_tags_aws_created_by, ''), 'aws:createdBy 태그 없음')
HAVING SUM(line_item_unblended_cost) > 0
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
HAVING SUM(line_item_unblended_cost) > 0
ORDER BY cost, service DESC;



-- -----------------------------------------------------------------------------
-- Q5. fetch_mtd_by_service_and_creator
--     CE: get_cost_and_usage(GroupBy=[SERVICE, TAG(aws:createdBy)], Granularity=MONTHLY)
--     data.py: fetch_mtd_by_service_and_creator(ce, period_mtd_this)  → by_creator_mtd
--     결과:  {service: {creator_label: float}}
--
--     MTD 범위: {mtd_start} ~ {d1_date} (inclusive)
--     당월 1일에 실행된 경우 범위가 비므로 Python에서 {} 반환 (이 쿼리 미실행)
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
  AND DATE(line_item_usage_start_date) BETWEEN DATE('2026-04-01') AND DATE('2026-04-05')
GROUP BY
    product_product_name,
    COALESCE(NULLIF(resource_tags_aws_created_by, ''), 'aws:createdBy 태그 없음')
HAVING SUM(line_item_unblended_cost) > 0
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
HAVING SUM(line_item_unblended_cost) > 0
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
