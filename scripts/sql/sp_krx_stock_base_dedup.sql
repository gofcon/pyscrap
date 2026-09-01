CREATE OR REPLACE PROCEDURE sp_krx_stock_base_dedup (p_deleted OUT NUMBER) AS
-- krx_stock_base 를 종목당 한 행으로 접는다. 남기는 것은 가장 최근 기준일.
--
-- 이 표는 시세가 아니라 종목이 무엇인가를 적은 것이다 -- 상장일, 시장, 주식
-- 종류, 액면가, 상장주식수. 거래소가 일자별로 주기 때문에 수집도 일자별로
-- 하지만, 값은 상장주식수가 움직일 때 말고는 매일 같다. 그대로 두면 하루
-- 943 행씩, 한 해 0.2GB 가 같은 내용으로 쌓이고, 2010년까지 이행하면 2GB 다.
--
-- 그래서 수집한 뒤 접는다. 접는 키는 isu_cd(표준코드)다 -- 단축코드는 재사용
-- 되지만 ISIN 은 종목에 붙어 바뀌지 않는다.
--
-- 남기는 것을 '최근' 으로 정한 이유: 상장주식수처럼 움직이는 값은 마지막에 본
-- 것이 지금 맞는 값이다. 상장폐지된 종목은 마지막으로 목록에 있던 날의 모습이
-- 남는데, 그것이 그 종목에 대해 알 수 있는 마지막 사실이다.
--
-- 그래서 과거 이행이 의미가 있다: 오늘 목록에는 없는, 이미 사라진 종목을
-- 거기서만 얻는다. 이행 중에도 이 프로시저를 날마다 부르므로 표는 새로 발견된
-- 종목 수만큼만 늘어난다.
--
-- 같은 날짜가 두 잡으로 두 번 들어오는 것도 여기서 접힌다(수집 잡과 VERIFY
-- 잡이 그랬다). save_mode='overwrite' 는 같은 job_id 의 이전 결과만 지우므로
-- 잡이 다르면 둘 다 남는다.
--
-- id 로 한 번 더 가르는 이유: 같은 기준일이 두 벌이면 bas_dd 만으로는 순서가
-- 정해지지 않아 ROW_NUMBER 가 임의로 고른다. 나중에 들어온 쪽을 택한다.
BEGIN
  DELETE /*+ NO_PARALLEL */ FROM krx_stock_base
   WHERE id IN (
     SELECT id FROM (
       SELECT id, ROW_NUMBER() OVER (PARTITION BY isu_cd
                                     ORDER BY bas_dd DESC, id DESC) rn
         FROM krx_stock_base)
      WHERE rn > 1);

  p_deleted := SQL%ROWCOUNT;
  DBMS_OUTPUT.PUT_LINE('krx_stock_base: ' || TO_CHAR(p_deleted, 'FM999,999,999')
                       || ' 중복 행 삭제');
END;
