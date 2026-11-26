CREATE OR REPLACE PROCEDURE sp_archive_exported (
  p_month     IN  VARCHAR2 DEFAULT NULL,  -- 대상 월 YYYYMM (NULL 이면 직전월)
  p_deleted   OUT NUMBER                  -- 지운 행 수 합계
) AS
-- 한 달치 수집분을 Parquet 으로 접고 원본에서 지운다. 매월 1일에 도는 작업이다.
--
-- 이 프로시저는 이제 **순서만 정한다.** 대상마다 두 가지를 차례로 부른다:
--
--   sp_compact_exported   그 달을 <대상>/_arch/<시작>_<끝>_part 하나로 만들고
--                         날짜별 파일을 접어 넣는다. DB 에 손대지 않는다.
--   sp_purge_exported     그 달이 버킷에서 읽히는 것을 확인하고 DB 에서 지운다.
--                         버킷에 손대지 않는다.
--
-- 전에는 이 둘이 한 몸이었고, 그 용접이 복잡함의 원인이었다 -- 내보낸 직후엔
-- 날짜별과 기간 파일이 둘 다 있는 중간 상태가 생겨 검증이 두 단계(먼저 '원본
-- 이상', 접은 뒤 '정확히 같음')여야 했다. 가르고 나니 각자 검증이 한 번이고,
-- 멈췄을 때 무엇이 끝났고 무엇이 안 끝났는지가 이름으로 읽힌다.
--
-- 순서가 중요하다: 접기가 먼저다. 접은 결과를 보고 지운다. 거꾸로 하면
-- 지운 뒤에 접을 것이 DB 에 없다 -- sp_compact_exported 는 DB 에서 내보낸다.
--
-- 왜 필요한가: kis_futopt_price 는 하루 87,000 행, kis_futopt_chart 는 33,000
-- 행씩 쌓인다. 한 달이면 각각 1GB, 0.5GB 다. 값은 Parquet 으로 나가 외부
-- 테이블로 읽히므로 DB 에 두 벌을 들고 있을 이유가 없다 -- 2026-09-01 에 이
-- 셋을 손으로 비워 4.48GB 를 0.85GB 로 줄였고, 그 절차를 옮긴 것이 이것이다.
--
-- 기준이 '며칠 지난 것' 이 아니라 '어느 달' 이다. 1일에 돌면서 직전월을
-- 통째로 가져가므로 아카이브 파일 하나가 달력의 한 달과 정확히 맞고, 언제
-- 돌리든 -- 다시 돌리든 -- 같은 구간을 본다. 일수로 자르면 실행 시각에 따라
-- 경계가 흔들려 한 달이 두 파일에 걸친다.
--
-- 다시 불러도 결과가 같다: sp_compact_exported 가 겹치는 기간 파일을 교체하고,
-- sp_purge_exported 는 남은 것만 다시 확인하고 지운다.
--
-- 그 대신 못 돌고 지나간 달은 저절로 따라잡히지 않는다. 두 달을 건너뛰었다면
-- p_month 에 그 달을 주고 한 번 더 부른다. 자동으로 '남은 것 전부' 를 쓸어가게
-- 하지 않는 이유는, 그 동작이 필요한 상황이 곧 뭔가 잘못된 상황이라서다 --
-- 사람이 보고 부르는 편이 낫다.
--
-- v2(수집 계정)에서는 이 타이머를 켜지 않는다. 검증용 창은 이제
-- sp_purge_exported 가 자기가 만들어 쓰므로 '계정에 외부 테이블이 미리 있어야
-- 한다'는 의존은 없어졌지만, 아카이브 자체가 분석 계정 몫이다 -- 수집 계정은
-- 모으고 내보내는 데까지 한다 (docs/SCHEMA-V2.md 7.6).
  TYPE t_names IS TABLE OF VARCHAR2(30);
  -- 손으로 적는다. 자동 발견(기간 파일이 있는 표 전부)으로 하면 언젠가
  -- 의도치 않은 표가 목록에 들어오고, 그 사고의 결과가 '데이터 삭제' 다.
  -- krx_opt_daily 는 2026-09-19 에 들어왔다. 옵션 전 종목이 하루 한 행씩이라
  -- 16년치 865만 행이 표와 인덱스로 3.3GB 였고, Parquet 으로는 127MB 다. 그날
  -- 2010-01-04..2026-09-01 전체를 기간 파일로 내보내 외부 테이블과 날짜별
  -- 행수·수치 합계를 대조한 뒤 표를 비웠다. 이후 일별 수집이 붙으면 달마다
  -- 여기서 _arch 로 옮겨 간다. 다른 KRX 일별 표는 넣지 않는다 -- v_fut_cont 가
  -- krx_fut_daily 를 직접 읽고, 외부 테이블 위에서는 그 뷰가 수십 배 느리다.
  v_targets t_names := t_names('kis_futopt_chart', 'kis_futopt_price', 'kis_futopt_daily',
                               'krx_opt_daily');
  v_kst     DATE;
  v_month   VARCHAR2(6);
  v_from    VARCHAR2(8);
  v_to      VARCHAR2(8);
  v_rows    NUMBER;
  v_del     NUMBER;
BEGIN
  p_deleted := 0;
  -- '직전월' 은 한국 기준이다. SYSDATE 는 DB 서버의 OS 시각이라 지금은 UTC 를
  -- 돌려주지만(DBTIMEZONE +00:00) 그것은 설정이지 약속이 아니다 -- 9/24 를
  -- 더하는 식은 그 설정이 바뀌면 조용히 어긋나고, 하필 매월 1일 04:00 KST 는
  -- UTC 로 전달 19:00 이라 달이 통째로 밀린다. 이름 붙은 시간대로 변환하면
  -- 서버가 어디에 있든 같은 답이 나온다 (sp_export_parquet 도 같은 식이다).
  v_kst   := CAST(SYSTIMESTAMP AT TIME ZONE 'Asia/Seoul' AS DATE);
  v_month := NVL(p_month, TO_CHAR(ADD_MONTHS(TRUNC(v_kst, 'MM'), -1), 'YYYYMM'));
  v_from  := v_month || '01';
  v_to    := TO_CHAR(LAST_DAY(TO_DATE(v_from, 'YYYYMMDD')), 'YYYYMMDD');

  DBMS_OUTPUT.PUT_LINE('sp_archive_exported ' || v_month
                       || ' (' || v_from || '..' || v_to || ')');

  FOR i IN 1 .. v_targets.COUNT LOOP
    sp_compact_exported(p_name => v_targets(i), p_to => v_to, p_from => v_from,
                        p_rows => v_rows);

    -- 접을 것이 없었으면 지울 것도 없다. 그 달에 행이 없다는 뜻이다 --
    -- sp_purge_exported 도 같은 판정을 하지만 창을 열기 전에 끊는 쪽이 싸다.
    IF v_rows > 0 THEN
      sp_purge_exported(p_name => v_targets(i), p_to => v_to, p_from => v_from,
                        p_deleted => v_del);
      p_deleted := p_deleted + v_del;
    END IF;
  END LOOP;
END;
