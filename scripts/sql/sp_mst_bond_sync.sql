CREATE OR REPLACE PROCEDURE sp_mst_bond_sync (p_inserted OUT NUMBER) AS
-- ksd_bond_isin(원본 목록, 매일 전량 append)에서 mst_bond(정제 마스터)로
-- 신규 ISIN 만 적재. 기존 isin 은 건드리지 않음 -- mst_bond 에 사람이 손댄
-- 값이 있어도 덮어쓰지 않기 위함. sp_mst_fuopt_sync 와 같은 방식이다.
--
-- 원본이 '매일 전량' 이라는 점이 이 프로시저가 필요한 이유다. SEIBRO 응답에는
-- 어느 종목이 새로 상장됐는지가 없고, 어제 목록에 없던 것이 오늘 있다는 사실
-- 로만 드러난다. 스냅샷을 쌓아 두고 여기서 접으면 그 차이가 first_seen 한
-- 컬럼으로 남아, 이후 배치는 스냅샷 두 개를 비교할 필요 없이 이 표만 보면 된다.
--
-- first_seen 은 '그 종목이 처음 보인 스냅샷 일자' 이지 발행일이 아니다. 수집을
-- 시작한 날 이미 상장돼 있던 종목은 전부 그날짜를 갖는다 -- 최초 1회는 전량이
-- 신규로 들어오고, 그다음부터가 진짜 신규다.
BEGIN
  MERGE INTO mst_bond t
  USING (
    -- 한 ISIN 이 스냅샷마다 한 행씩 있으므로 1건으로 접는다. 접지 않으면
    -- MERGE 가 소스 키 중복으로 ORA-30926 을 낸다.
    --
    -- 값은 '가장 최근 스냅샷' 것을 쓴다. 종목명이나 분류가 바뀌면 새 값이
    -- 맞고, 어차피 신규 INSERT 에만 쓰이므로 그 종목이 처음 보인 날의 값과
    -- 거의 같다. first_seen 만 MIN 으로 가장 오래된 관측일을 잡는다.
    SELECT isin,
           MIN(day)          AS first_seen,
           MAX(kor_secn_nm)   KEEP (DENSE_RANK LAST ORDER BY day) AS kor_secn_nm,
           MAX(secn_kacd)     KEEP (DENSE_RANK LAST ORDER BY day) AS secn_kacd,
           MAX(codevalue_nm)  KEEP (DENSE_RANK LAST ORDER BY day) AS codevalue_nm,
           MAX(issuco_custno) KEEP (DENSE_RANK LAST ORDER BY day) AS issuco_custno,
           MAX(issu_dt)       KEEP (DENSE_RANK LAST ORDER BY day) AS issu_dt
      FROM (SELECT i.isin, SUBSTR(i.trade_at, 1, 8) AS day,
                   i.kor_secn_nm, i.secn_kacd, i.codevalue_nm,
                   i.issuco_custno, i.issu_dt
              FROM ksd_bond_isin i
             WHERE i.trade_at IS NOT NULL)
     GROUP BY isin
  ) s
  ON (t.isin = s.isin)
  WHEN NOT MATCHED THEN
    INSERT (isin, kor_secn_nm, secn_kacd, codevalue_nm, issuco_custno,
            issu_dt, first_seen)
    VALUES (s.isin, s.kor_secn_nm, s.secn_kacd, s.codevalue_nm, s.issuco_custno,
            s.issu_dt, s.first_seen);

  p_inserted := SQL%ROWCOUNT;
END;
