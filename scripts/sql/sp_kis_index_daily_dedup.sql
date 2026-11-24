CREATE OR REPLACE PROCEDURE sp_kis_index_daily_dedup (p_deleted OUT NUMBER) AS
-- kis_index_daily 를 (영업일, 지수코드) 당 한 행으로 접는다. 남기는 것은 가장
-- 최근에 적재된 것.
--
-- sp_stock_index_his_sync 의 MERGE 가 이 표를 소스로 읽으므로 그 키가 한
-- 건이어야 한다 -- 중복이 있으면 ORA-30926 이다.
--
-- v1 에서는 이 DELETE 가 sp_stock_index_his_sync 안에 있었다. v2 는 계정이
-- 갈려서 그럴 수 없다: 그 프로시저는 REF 소유이고 kis_index_daily 는 SCRAP
-- 소유다. REF 에 DELETE 를 주면 사전 데이터를 만드는 쪽이 수집물을 지울 수
-- 있게 되는데, 계정을 나눈 이유가 바로 그것을 막는 것이다.
--
-- 그래서 sp_krx_stock_base_dedup 과 같은 모양으로 떼어냈다 -- 수집 표를
-- 청소하는 일은 수집 계정 몫이고, 그쪽에 이미 선례가 있다. 배치는 sync 앞에
-- 이것을 부른다.
--
-- 겹침이 생기는 경로: 빌더가 롤링 기간을 쓰던 때 쌓인 것과, save_mode=
-- overwrite 가 '같은 job_id 의 이전 결과' 만 지우는 성질. 지금 빌더는
-- 하루짜리라 새로 생기지는 않지만, 옛 행이 남아 있고 기간을 다시 넓힐 여지도
-- 있어 접는 쪽을 둔다.
--
-- 나눠서 지우고 중간에 커밋하는 것도 sp_krx_stock_base_dedup 과 같은 이유다
-- (밀렸을 때 ORA-30036 을 피한다). 되돌릴 일이 없는 청소라 원자성을 포기하는
-- 대가가 없다.
  c_batch CONSTANT PLS_INTEGER := 50000;
  v_n     PLS_INTEGER;
BEGIN
  p_deleted := 0;
  LOOP
    DELETE /*+ NO_PARALLEL */ FROM kis_index_daily
     WHERE id IN (
       SELECT id FROM (
         SELECT id, ROW_NUMBER() OVER (PARTITION BY stck_bsop_date, short_code
                                       ORDER BY updated_at DESC, id DESC) rn
           FROM kis_index_daily)
        WHERE rn > 1)
       AND ROWNUM <= c_batch;

    v_n := SQL%ROWCOUNT;
    EXIT WHEN v_n = 0;
    p_deleted := p_deleted + v_n;
    COMMIT;
  END LOOP;

  DBMS_OUTPUT.PUT_LINE('kis_index_daily: ' || TO_CHAR(p_deleted, 'FM999,999,999')
                       || ' 중복 행 삭제');
END;
