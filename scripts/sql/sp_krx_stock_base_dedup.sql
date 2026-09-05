CREATE OR REPLACE PROCEDURE sp_krx_stock_base_dedup (p_deleted OUT NUMBER) AS
-- krx_stock_base 를 종목당 한 행으로 접는다. 남기는 것은 가장 최근 기준일.
--
-- 이 표는 시세가 아니라 종목이 무엇인가를 적은 것이다 -- 상장일, 시장, 주식
-- 종류, 액면가, 상장주식수. 거래소가 일자별로 주기 때문에 수집도 일자별로
-- 하지만, 값은 상장주식수가 움직일 때 말고는 매일 같다. 그대로 두면 하루
-- 943 행씩, 한 해 0.2GB 가 같은 내용으로 쌓이고, 2010년까지 이행하면 2GB 다.
--
-- 접는 키는 isu_cd(표준코드)다 -- 단축코드는 재사용되지만 ISIN 은 종목에 붙어
-- 바뀌지 않는다.
--
-- 남기는 것을 '최근' 으로 정한 이유: 상장주식수처럼 움직이는 값은 마지막에 본
-- 것이 지금 맞는 값이다. 상장폐지된 종목은 마지막으로 목록에 있던 날의 모습이
-- 남는데, 그것이 그 종목에 대해 알 수 있는 마지막 사실이다.
--
-- 나눠서 지우고 중간에 커밋한다. 다른 프로시저들은 커밋하지 않는 것이 규칙이나
-- (호출하는 쪽이 다른 작업과 묶을 수 있도록) 여기는 예외다: 과거 이행이 밀려
-- 1,164,727 행을 한 번에 지워야 하는 상황이 실제로 벌어졌고, 한 문장으로는
-- ORA-30036(undo 부족)으로 죽었다. 되돌릴 일이 없는 청소 작업이라 -- 지우는
-- 것은 어차피 중복이고, 중간에 멈춰도 다음 실행이 이어서 지운다 -- 원자성을
-- 포기하는 대가가 없다.
--
-- 매일 도는 경우는 943 행이라 한 배치에 끝난다. 배치 크기는 그 정상 상태가
-- 아니라 밀렸을 때를 위한 것이다.
  c_batch CONSTANT PLS_INTEGER := 50000;
  v_n     PLS_INTEGER;
BEGIN
  p_deleted := 0;
  LOOP
    DELETE /*+ NO_PARALLEL */ FROM krx_stock_base
     WHERE id IN (
       SELECT id FROM (
         SELECT id, ROW_NUMBER() OVER (PARTITION BY isu_cd
                                       ORDER BY bas_dd DESC, id DESC) rn
           FROM krx_stock_base)
        WHERE rn > 1)
       AND ROWNUM <= c_batch;

    v_n := SQL%ROWCOUNT;
    EXIT WHEN v_n = 0;
    p_deleted := p_deleted + v_n;
    COMMIT;
  END LOOP;

  DBMS_OUTPUT.PUT_LINE('krx_stock_base: ' || TO_CHAR(p_deleted, 'FM999,999,999')
                       || ' 중복 행 삭제');
END;
