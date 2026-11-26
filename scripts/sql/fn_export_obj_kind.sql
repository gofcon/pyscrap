CREATE OR REPLACE FUNCTION fn_export_obj_kind (
  p_object_name IN VARCHAR2                 -- LIST_OBJECTS 가 준 상대경로
) RETURN VARCHAR2 DETERMINISTIC AS
-- 버킷 객체 이름 하나가 어느 꼴인지 판정한다. 'DAY' | 'RANGE' | 'OTHER'.
--
-- 왜 함수로 뺐나. 이 판정이 세 프로시저에 자릿수 산수로 복사돼 있었다 --
-- INSTR(object_name, '/') = 9 가 두 벌(sp_export_bulk, sp_archive_exported),
-- SUBSTR(object_name, 10, 8) 이 또 한 벌(sp_export_parquet). 고칠 자리가 세
-- 곳이면 레이아웃을 손볼 때 언젠가 한 곳이 빠지고, 그 한 곳이 '지울 것'
-- 판정이면 데이터가 사라진다. 한 곳으로 모은다.
--
-- 받는 것은 LIST_OBJECTS 의 상대경로다. 프리픽스를 <대상>/ 으로 줬든
-- <대상>/_arch/ 로 줬든 같은 답이 나오게, 앞에 '_arch/' 가 있으면 떼고 본다 --
-- 전자는 '_arch/20261101_20261130_part_1_x.parquet' 로 오고 후자는
-- '20261101_20261130_part_1_x.parquet' 로 온다. 두 프리픽스를 다 쓰는 이유는
-- 비용이다: 겹침만 보려면 '_arch/' 로 물어 기간 파일 몇 개만 받는 것이
-- 대상 폴더 전체(몇 년치면 수천 건)를 훑는 것보다 싸다.
--
-- 두 꼴은 9번째 글자로 갈린다. 날짜별은 폴더 경계라 '/' 이고, 기간은 시작과
-- 끝을 밑줄로 이어 '_' 다.
--
--   20261126/20261126_part_1_x.parquet        -> DAY    (하루)
--   _arch/20261101_20261130_part_1_x.parquet  -> RANGE  (기간)
--
-- 'OTHER' 를 돌려주는 것들: 대상 폴더 바로 밑에 놓인 파일, 사람이 손으로
-- 올린 것, 그리고 v1 의 옛 '_arch/<YYYYMM>_part_...' 꼴. 마지막 것을 일부러
-- 인식하지 않는다 -- v2 의 _arch 에는 이 프로시저들만 쓰므로 그 꼴이 생길 수
-- 없고, v1 은 자기 DB 에 자기 옛 프로시저를 들고 있어 이 함수를 부르지
-- 않는다. 넓혀 두면 존재하지 않는 경우를 영구히 들고 가게 된다.
--
-- 'OTHER' 는 겹침 판정에서 'N' 이 되므로(fn_export_obj_overlaps), 모르는
-- 객체는 지우지도 겹친다고 보지도 않는다. 모를 때 손대지 않는 쪽이 맞다.
--
-- SQL 에서 부를 수 있게 BOOLEAN 이 아니라 문자열을 쓴다. 호출부가
-- LIST_OBJECTS 위의 WHERE 절이라 그게 중첩 IF 보다 읽힌다.
  c_name CONSTANT VARCHAR2(4000) :=
    CASE WHEN p_object_name LIKE '\_arch/%' ESCAPE '\'
         THEN SUBSTR(p_object_name, 7)
         ELSE p_object_name END;
BEGIN
  IF NOT REGEXP_LIKE(SUBSTR(c_name, 1, 8), '^\d{8}$') THEN
    RETURN 'OTHER';
  END IF;

  IF SUBSTR(c_name, 9, 1) = '/' THEN
    RETURN 'DAY';
  END IF;

  IF SUBSTR(c_name, 9, 1) = '_'
     AND REGEXP_LIKE(SUBSTR(c_name, 10, 8), '^\d{8}$') THEN
    RETURN 'RANGE';
  END IF;

  RETURN 'OTHER';
END;
