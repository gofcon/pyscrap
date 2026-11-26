CREATE OR REPLACE FUNCTION fn_export_obj_overlaps (
  p_object_name IN VARCHAR2,                -- LIST_OBJECTS 가 준 상대경로
  p_from        IN VARCHAR2,                -- YYYYMMDD, 포함
  p_to          IN VARCHAR2                 -- YYYYMMDD, 포함
) RETURN VARCHAR2 DETERMINISTIC AS
-- 그 객체가 담은 날짜 구간이 [p_from, p_to] 와 겹치는가. 'Y' / 'N'.
--
-- 내보내기 쪽에서 버킷에 묻는 것은 사실상 전부 이 질문이다. 2026-11-26 전에는
-- 네 자리에서 네 가지 산수로 물었다 (이름은 그때 것이다 -- 앞의 둘이 있던
-- 자리는 지금 없다):
--   sp_export_bulk       "범위 안의 날짜 폴더인가"      -> 지운다(접는다)
--   sp_archive_exported  같은 질문                      -> 지운다(접는다)
--   sp_archive_exported  "이 달의 _arch 인가"           -> 지운다(다시 쓰기 전)
--   sp_export_parquet    "기간 파일이 내 날과 겹치나"   -> -20002 로 거절
-- 네 개가 한 함수가 된다. 지금 부르는 쪽은 sp_export_parquet 과
-- sp_compact_exported 다. 꼴(DAY/RANGE)은 fn_export_obj_kind 가 판정한다.
--
-- 구간은 파일 이름이 들고 있다 -- 어디까지 기간으로 나갔는지를 별도 표에
-- 적어 두면 누가 버킷에서 파일을 지웠을 때 그 기록만 남아 어긋난다. 버킷을
-- 유일한 사실로 둔다.
--
-- 겹침 판정은 양끝 포함이다: 두 구간이 한 날이라도 공유하면 'Y'. 이것이
-- 중복의 정의다 -- 외부 테이블이 <대상>/*/*.parquet 하나로 날짜별과 기간
-- 파일을 같이 읽으므로, 겹친 날은 두 번 세어진다.
--
-- 'OTHER' 는 'N' 이다. 모르는 객체를 지우지도, 겹친다고 보지도 않는다 --
-- 'Y' 로 기울면 모르는 파일을 지우고, 그 실수의 결과가 데이터 손실이다.
--
-- 날짜 비교를 문자열로 한다. YYYYMMDD 는 사전순이 곧 시간순이라 TO_DATE 가
-- 필요 없고, 변환을 넣으면 그 자리가 NLS_DATE_FORMAT 에 묶인다.
  c_kind CONSTANT VARCHAR2(5)    := fn_export_obj_kind(p_object_name);
  c_name CONSTANT VARCHAR2(4000) :=
    CASE WHEN p_object_name LIKE '\_arch/%' ESCAPE '\'
         THEN SUBSTR(p_object_name, 7)
         ELSE p_object_name END;
  v_from VARCHAR2(8);
  v_to   VARCHAR2(8);
BEGIN
  IF c_kind = 'OTHER' THEN
    RETURN 'N';
  END IF;

  v_from := SUBSTR(c_name, 1, 8);
  v_to   := CASE c_kind WHEN 'DAY' THEN v_from ELSE SUBSTR(c_name, 10, 8) END;

  RETURN CASE WHEN v_from <= p_to AND v_to >= p_from THEN 'Y' ELSE 'N' END;
END;
