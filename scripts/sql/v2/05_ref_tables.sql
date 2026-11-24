-- REF 계정: 사전 데이터 테이블 11개
--
-- gen_ddl.py 가 app/db/models.py 에서 뽑은 것이다. 손으로 고치지 말고
-- 모델을 고친 뒤 다시 뽑는다.
--   python scripts/sql/v2/gen_ddl.py


-- 엔진은 읽기만 한다. 채우는 것은 sp_*_sync 프로시저이고, 그 둘을
-- 계정으로 가르면 권한이 그 사실을 강제한다 -- 파이썬이 실수로
-- mst_etf 에 INSERT 할 수 없다.
--
-- user_etf 가 이 계정을 나누는 가장 큰 이유다. 손으로 적은 값이라
-- 다시 만들 수 없는데 v1 에서는 날마다 비워지는 결과 테이블과 같은
-- 계정에 있었다.

-- krx_index_mst: KRX's own index master -- every index the exchange publishes, as it
CREATE TABLE krx_index_mst (
	ind_tp_cd VARCHAR2(4 CHAR) NOT NULL, 
	idx_ind_cd VARCHAR2(4 CHAR) NOT NULL, 
	idx_nm VARCHAR2(100 CHAR) NOT NULL, 
	idx_eng_nm VARCHAR2(150 CHAR), 
	mid_clss_cd VARCHAR2(2 CHAR), 
	bas_tm VARCHAR2(10 CHAR), 
	annc_tm VARCHAR2(10 CHAR), 
	bas_idx FLOAT, 
	calc_cycle VARCHAR2(20 CHAR), 
	calc_tm VARCHAR2(40 CHAR), 
	compst_isu_cnt INTEGER, 
	mv_id VARCHAR2(10 CHAR), 
	description VARCHAR2(200 CHAR), 
	updated_at DATE DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (ind_tp_cd, idx_ind_cd)
);

CREATE INDEX ix_krx_index_mst_idx_nm ON krx_index_mst (idx_nm);
CREATE INDEX ix_krx_index_mst_mv_id ON krx_index_mst (mv_id);

-- ksd_bond_no_detail: ISINs SEIBRO has no detail for -- so the migration stops asking them.
CREATE TABLE ksd_bond_no_detail (
	isin VARCHAR2(12 CHAR) NOT NULL, 
	marked_at DATE, 
	note VARCHAR2(100 CHAR), 
	PRIMARY KEY (isin)
);


-- meta_fuopt_info: What each product family is written on: one row per (``prod_type``,
CREATE TABLE meta_fuopt_info (
	prod_type VARCHAR2(20 CHAR) NOT NULL, 
	ul_code VARCHAR2(20 CHAR) NOT NULL, 
	ul_nm VARCHAR2(100 CHAR) NOT NULL, 
	krx_idx_nm VARCHAR2(100 CHAR), 
	mv_id VARCHAR2(20 CHAR), 
	cont_mult FLOAT NOT NULL, 
	code_key VARCHAR2(2 CHAR) NOT NULL, 
	exp_dow VARCHAR2(9 CHAR) NOT NULL, 
	exp_week INTEGER, 
	krx_prod_ids VARCHAR2(100 CHAR), 
	kis_info_types VARCHAR2(100 CHAR), 
	description VARCHAR2(100 CHAR), 
	updated_at DATE DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (prod_type, ul_code), 
	UNIQUE (code_key)
);


-- meta_maturity: Contract expiry calendar: one row per maturity of one product type.
CREATE TABLE meta_maturity (
	prod_type VARCHAR2(20 CHAR) NOT NULL, 
	mat_code VARCHAR2(20 CHAR) NOT NULL, 
	mat_date DATE, 
	prev_mat_date DATE, 
	mat_scd VARCHAR2(20 CHAR), 
	description VARCHAR2(100 CHAR), 
	updated_at DATE DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (prod_type, mat_code)
);

CREATE INDEX ix_meta_maturity_mat_date ON meta_maturity (mat_date);
CREATE INDEX ix_meta_maturity_mat_scd ON meta_maturity (mat_scd);

-- mst_bond: Curated bond instrument master -- the list of issues worth collecting
CREATE TABLE mst_bond (
	isin VARCHAR2(12 CHAR) NOT NULL, 
	kor_secn_nm VARCHAR2(200 CHAR), 
	secn_kacd VARCHAR2(10 CHAR), 
	codevalue_nm VARCHAR2(100 CHAR), 
	issuco_custno VARCHAR2(20 CHAR), 
	issu_dt VARCHAR2(8 CHAR), 
	first_seen VARCHAR2(8 CHAR), 
	description VARCHAR2(100 CHAR), 
	updated_at DATE DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (isin)
);

CREATE INDEX ix_mst_bond_first_seen ON mst_bond (first_seen);
CREATE INDEX ix_mst_bond_issu_dt ON mst_bond (issu_dt);
CREATE INDEX ix_mst_bond_secn_kacd ON mst_bond (secn_kacd);

-- mst_etf: Curated ETF instrument master -- the universe with each manager's own
CREATE TABLE mst_etf (
	short_code VARCHAR2(20 CHAR) NOT NULL, 
	isin VARCHAR2(12 CHAR), 
	prod_nm VARCHAR2(200 CHAR), 
	prod_snm VARCHAR2(100 CHAR), 
	prod_enm VARCHAR2(200 CHAR), 
	list_dt VARCHAR2(8 CHAR), 
	target_idx_nm VARCHAR2(200 CHAR), 
	trace_mtd VARCHAR2(20 CHAR), 
	replica_mtd VARCHAR2(40 CHAR), 
	mkt_div VARCHAR2(20 CHAR), 
	idx_asst_class VARCHAR2(20 CHAR), 
	amc VARCHAR2(40 CHAR), 
	amc_etf_cd VARCHAR2(30 CHAR), 
	cu_qty INTEGER, 
	list_cnt INTEGER, 
	etf_tot_fee FLOAT, 
	tax_div VARCHAR2(40 CHAR), 
	first_seen VARCHAR2(8 CHAR), 
	description VARCHAR2(100 CHAR), 
	updated_at DATE DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (short_code)
);

CREATE INDEX ix_mst_etf_amc ON mst_etf (amc);
CREATE INDEX ix_mst_etf_first_seen ON mst_etf (first_seen);
CREATE INDEX ix_mst_etf_isin ON mst_etf (isin);

-- mst_fuopt: Curated futures/options instrument master -- the list of contracts
CREATE TABLE mst_fuopt (
	short_code VARCHAR2(10 CHAR) NOT NULL, 
	kis_short_cd VARCHAR2(10 CHAR), 
	prod_nm VARCHAR2(100 CHAR) NOT NULL, 
	prod_type VARCHAR2(20 CHAR) NOT NULL, 
	call_put_cd VARCHAR2(20 CHAR) NOT NULL, 
	ul_code VARCHAR2(20 CHAR), 
	ul_nm VARCHAR2(100 CHAR), 
	cont_mult FLOAT NOT NULL, 
	mat_code VARCHAR2(20 CHAR), 
	mat_date DATE, 
	front_date DATE, 
	strike_prc FLOAT, 
	description VARCHAR2(100 CHAR), 
	updated_at DATE DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (short_code)
);

CREATE INDEX ix_mst_fuopt_front_date ON mst_fuopt (front_date);
CREATE INDEX ix_mst_fuopt_kis_short_cd ON mst_fuopt (kis_short_cd);
CREATE INDEX ix_mst_fuopt_mat_date ON mst_fuopt (mat_date);
CREATE INDEX ix_mst_fuopt_ul_code ON mst_fuopt (ul_code);

-- mst_index: Curated index master -- the indices this project actually follows, one
CREATE TABLE mst_index (
	mv_id VARCHAR2(10 CHAR) NOT NULL, 
	idx_nm VARCHAR2(100 CHAR) NOT NULL, 
	idx_eng_nm VARCHAR2(150 CHAR), 
	bas_tm VARCHAR2(10 CHAR), 
	annc_tm VARCHAR2(10 CHAR), 
	bas_idx FLOAT, 
	kis_short_cd VARCHAR2(10 CHAR), 
	description VARCHAR2(200 CHAR), 
	updated_at DATE DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (mv_id)
);

CREATE INDEX ix_mst_index_idx_nm ON mst_index (idx_nm);
CREATE INDEX ix_mst_index_kis_short_cd ON mst_index (kis_short_cd);

-- mst_stock: Curated share instrument master -- every share the exchange has listed
CREATE TABLE mst_stock (
	short_code VARCHAR2(20 CHAR) NOT NULL, 
	isin VARCHAR2(12 CHAR), 
	prod_nm VARCHAR2(100 CHAR), 
	prod_snm VARCHAR2(60 CHAR), 
	prod_enm VARCHAR2(120 CHAR), 
	list_dt VARCHAR2(8 CHAR), 
	mkt_div VARCHAR2(20 CHAR), 
	secu_grp VARCHAR2(40 CHAR), 
	secu_type VARCHAR2(40 CHAR), 
	stock_kind_type VARCHAR2(20 CHAR), 
	face_amt VARCHAR2(20 CHAR), 
	list_cnt INTEGER, 
	last_seen_dt VARCHAR2(8 CHAR), 
	description VARCHAR2(200 CHAR), 
	updated_at DATE DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (short_code)
);

CREATE INDEX ix_mst_stock_isin ON mst_stock (isin);
CREATE INDEX ix_mst_stock_last_seen_dt ON mst_stock (last_seen_dt);
CREATE INDEX ix_mst_stock_mkt_div ON mst_stock (mkt_div);

-- stock_index_his: Daily OHLC/volume history for a stock index, one row per index per
CREATE TABLE stock_index_his (
	trade_date DATE NOT NULL, 
	mv_id VARCHAR2(20 CHAR) NOT NULL, 
	close_price FLOAT, 
	price_change FLOAT, 
	change_rate FLOAT, 
	open_price FLOAT, 
	high_price FLOAT, 
	low_price FLOAT, 
	volume INTEGER, 
	trading_value INTEGER, 
	listed_market_cap FLOAT, 
	updated_at DATE DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (trade_date, mv_id)
);

CREATE INDEX ix_stock_index_his_mv_id ON stock_index_his (mv_id);

-- user_etf: 운용사 ETF 코드 중 사람이 직접 넣는 것 -- mst_etf 로 접혀 들어간다.
CREATE TABLE user_etf (
	isu_cd VARCHAR2(20 CHAR) NOT NULL, 
	amc VARCHAR2(20 CHAR), 
	amc_etf_cd VARCHAR2(30 CHAR), 
	description VARCHAR2(100 CHAR), 
	updated_at DATE DEFAULT CURRENT_TIMESTAMP, 
	PRIMARY KEY (isu_cd)
);

CREATE INDEX ix_user_etf_amc ON user_etf (amc);
