import pandas as pd
import datetime
import re
import os
import time
from io import StringIO
import random
import glob

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
#from webdriver_manager.chrome import ChromeDriverManager
import yfinance as yf

from signals import (
    extract_oi_volume_signal,
    extract_iv_signal,
    extract_oi_signals,
    extract_pcr_signals,
    extract_max_pain_signals,
    extract_atm_focus_signal,
    detect_rollover_and_unwind_signals,
    detect_gamma_squeeze
)
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

#=====================================================

def main_crawler():
    today_str = datetime.datetime.now().strftime('%Y%m%d')
    file_today = f"market_data_report_{today_str}.xlsx"

    # --- WebDriver 옵션 설정 시작 ---
    chrome_options = Options()

    # 1. Headless 모드 활성화
    chrome_options.add_argument("--headless")

    # 2. 샌드박스 비활성화 (Linux/Docker 환경에서 종종 필요)
    # GitHub Actions는 Linux 기반이므로 추가하는 것이 좋습니다.
    chrome_options.add_argument("--no-sandbox")

    # 3. /dev/shm 사용 비활성화 (리소스 제한적인 Docker 환경에서 크래시 방지)
    # GitHub Actions 환경에서도 도움이 될 수 있습니다.
    chrome_options.add_argument("--disable-dev-shm-usage")

    # 4. GPU 가속 비활성화 (Headless 모드에서 불필요하거나 문제를 일으킬 수 있음)
    chrome_options.add_argument("--disable-gpu") # 선택 사항이지만, headless에서 종종 사용됨

    # 5. 가상 윈도우 크기 설정 (웹페이지 레이아웃에 영향)
    # 특정 해상도에서 웹사이트가 다르게 보일 수 있으므로,
    # 로컬에서 테스트하던 해상도와 유사하게 설정하면 예기치 않은 문제를 줄일 수 있습니다.
    chrome_options.add_argument("--window-size=1920,1080") # 예시 해상도

    # 6. 사용자 에이전트 설정 (선택 사항)
    # 일부 웹사이트는 특정 브라우저나 환경을 감지하여 다르게 동작할 수 있습니다.
    # 필요하다면 일반적인 데스크톱 브라우저의 User-Agent로 설정할 수 있습니다.
    # user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
    # chrome_options.add_argument(f'user-agent={user_agent}')

    # --- WebDriver 옵션 설정 끝 ---

    # 크롬 드라이버 실행 (수정된 부분)
    print("Initializing Chrome WebDriver with options...")
    try:
        # 시스템 PATH에 있는 ChromeDriver를 사용하도록 설정
        driver = webdriver.Chrome(options=chrome_options)
        print("WebDriver initialized successfully (using system chromedriver).")
    except Exception as e:
        print(f"Error initializing WebDriver with system chromedriver: {e}")
        # WebDriver 초기화 실패 시, 여기서 더 진행하기 어려우므로 예외를 다시 발생시킵니다.
        raise

    # 1. 만기요약(ExpirySummary) 크롤링 및 가공
    df_expiry = get_expiry_summary(driver, TICKERS)
    if not df_expiry.empty:
        df_expiry = flatten_columns(df_expiry)
        cols = df_expiry.columns.tolist()
        if "Ticker" in cols:
            cols.remove("Ticker")
            df_expiry = df_expiry[["Ticker"] + cols]

    # 2. 주가 데이터(HistoricalPrices) 크롤링 및 가공
    df_prices = get_historical_prices(TICKERS, HISTORICAL_DATA_PERIOD, TICKERS_TO_EXCLUDE_FROM_PRICES)
    if not df_prices.empty:
        df_prices = flatten_columns(df_prices)

    close_col = None
    for c in df_prices.columns:
        if "close" in c.lower():
            close_col = c
            break

    if not df_expiry.empty and "Ticker" in df_expiry.columns and close_col:
        latest_prices = df_prices.drop_duplicates("Ticker", keep="first")[["Ticker", close_col]]
        latest_prices = latest_prices.rename(columns={close_col: "Current Price"})
        df_expiry = df_expiry.merge(latest_prices, on="Ticker", how="left")
        if "Max Pain_Max Pain" in df_expiry.columns and "Current Price" in df_expiry.columns:
            df_expiry["Max Pain vs Current Price"] = df_expiry["Max Pain_Max Pain"] - df_expiry["Current Price"]
    else:
        print("[Warn] Close 컬럼을 찾지 못했습니다! 컬럼명:", df_prices.columns.tolist())

        cols = df_prices.columns.tolist()
        if "Ticker" in cols:
            cols.remove("Ticker")
            df_prices = df_prices[["Ticker"] + cols]
        if "Date" in df_prices.columns:
            df_prices = df_prices.sort_values("Date", ascending=False).reset_index(drop=True)

        if not df_expiry.empty and "Ticker" in df_expiry.columns:
            latest_prices = df_prices.drop_duplicates("Ticker", keep="first")[["Ticker", "Close"]]
            df_expiry = df_expiry.merge(latest_prices, on="Ticker", how="left")
            df_expiry = df_expiry.rename(columns={"Close": "Current Price"})
            if "Max Pain_Max Pain" in df_expiry.columns and "Current Price" in df_expiry.columns:
                df_expiry["Max Pain vs Current Price"] = (
                    df_expiry["Max Pain_Max Pain"] - df_expiry["Current Price"]
                )

    # 3. 옵션 체인 상세(OptionChains_Detail) 크롤링 및 저장
    BASE_URL_TEMPLATE = "https://optioncharts.io/options/{ticker}"
    df_chains = get_detailed_option_chains(driver, TICKERS, BASE_URL_TEMPLATE, OPTION_CHAIN_MAX_EXPIRY_DAYS)

    # ========== 엑셀 저장 (FileNotFoundError 절대 안 나게!) ==========
    file_created = False
    # 1. 만기요약 저장
    if not df_expiry.empty:
        with pd.ExcelWriter(file_today, mode='w', engine='openpyxl') as writer:
            df_expiry.to_excel(writer, sheet_name=f"ExpirySummary_{today_str}", index=False)
        file_created = True

    # 2. 주가 저장
    if not df_prices.empty:
        if "Date" in df_prices.columns and "Ticker" in df_prices.columns:
            df_prices = df_prices.sort_values(["Ticker", "Date"], ascending=[True, False]).reset_index(drop=True)
        save_mode = 'a' if file_created else 'w'
        with pd.ExcelWriter(file_today, mode=save_mode, engine='openpyxl') as writer:
            df_prices.to_excel(writer, sheet_name=f"HistoricalPrices_{today_str}", index=False)
        file_created = True

    # 3. 옵션체인 저장
    if not df_chains.empty:
        save_mode = 'a' if file_created else 'w'
        with pd.ExcelWriter(file_today, mode=save_mode, engine='openpyxl') as writer:
            df_chains.to_excel(writer, sheet_name=f"OptionChains_Detail_{today_str}", index=False)
    # ================================================================

    driver.quit()
    print("=== 크롤링 & 엑셀 저장 완료 ===")

# ======== 구글업로드 ========

def upload_to_google_sheets():
    today_str = datetime.datetime.now().strftime('%Y%m%d')
    file_today = f"market_data_report_{today_str}.xlsx"
    google_sheet_id = "12g_bvrzg-3PWA3zzIUsIdhIlKbDn_BMv9zRi53VfFQQ"  # 구글 시트 ID

    # 구글 인증
    gclient = authenticate_google_sheets(GOOGLE_API_KEY_FILE)

    # 1. 크롤링 데이터 시트 3개 업로드
    # 만기요약
    df_expiry = safe_load_excel(file_today, f"ExpirySummary_{today_str}")
    upload_df_to_gsheet(gclient, google_sheet_id, f"ExpirySummary_{today_str}", df_expiry)
    # 주가
    df_prices = safe_load_excel(file_today, f"HistoricalPrices_{today_str}")
    upload_df_to_gsheet(gclient, google_sheet_id, f"HistoricalPrices_{today_str}", df_prices)
    # 옵션체인
    df_chains = safe_load_excel(file_today, f"OptionChains_Detail_{today_str}")
    upload_df_to_gsheet(gclient, google_sheet_id, f"OptionChains_Detail_{today_str}", df_chains)



    # 2. 시그널 시트들 업로드
    # (저장된 시그널 엑셀 파일별로 자동 업로드)
    import glob
    signal_files = glob.glob(f"*_Signal_{today_str}.xlsx")
    for file in signal_files:
        try:
            sheet_name = file.replace(f"_{today_str}.xlsx", "")
            df_signal = pd.read_excel(file)
            upload_df_to_gsheet(gclient, google_sheet_id, sheet_name, df_signal)
        except Exception as e:
            print(f"[Google Sheets] {file} 업로드 실패: {e}")

    print("=== 구글시트 업로드 완료 ===")

# ======== 설정값 (실전 파라미터 한번에 관리) ========
TICKERS = ['SPY', 'QQQ', 'VIX', 'TLT', 'HYG', 'GLD', 'XLE', 'XLK', 'XLF', 'XLU', 'ARKK', 'SOXX', 'TSLA', 'NVDA', 'PLTR', 'MSTR', 'GOOGL']
TICKERS_TO_EXCLUDE_FROM_PRICES = ['VIX']
HISTORICAL_DATA_PERIOD = '1y'
OPTION_CHAIN_MAX_EXPIRY_DAYS = 183

GOOGLE_API_KEY_FILE = "sb.json"
GOOGLE_SHEET_ID = "12g_bvrzg-3PWA3zzIUsIdhIlKbDn_BMv9zRi53VfFQQ"
SAVE_TO_GOOGLE_SHEETS = True

# --- 시그널 기준값(수정 필요시 여기만 수정) ---
SIG_OI_CHANGE_PCT = 20    # OI 변화 기준 (%)
SIG_VOLUME_THRESHOLD = 500 # 볼륨 기준
SIG_IV_JUMP = 20          # IV 급등락 %
SIG_PCR_UPPER = 2.5       # PCR 극단값 상단
SIG_PCR_LOWER = 0.4       # PCR 극단값 하단
SIG_ATM_WINDOW = 1        # ATM ± window
SIG_MAXPAIN_DEVIATION = 0.03 # MaxPain 대비 괴리(3%)

# ======== 유틸 함수 ========
def flatten_columns(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            "_".join([str(col_part).replace("Unnamed: ", "Col") for col_part in col if str(col_part) not in ('', 'nan')]).strip("_")
            for col in df.columns.values
        ]
    return df

def file_exists(filepath):
    return os.path.exists(filepath) and os.path.getsize(filepath) > 0

import glob

def safe_load_excel(file_pattern, sheet_pattern, max_days=7):
    # 오늘~max_days 전까지 파일/시트 탐색, 없으면 폴더 내 가장 최신 파일 사용
    for d in range(0, max_days):
        dt = datetime.datetime.now() - datetime.timedelta(days=d)
        dt_str = dt.strftime('%Y%m%d')
        fname = file_pattern.replace('%Y%m%d', dt_str)
        sname = sheet_pattern.replace('%Y%m%d', dt_str)
        if file_exists(fname):
            try:
                xl = pd.ExcelFile(fname)
                if sname in xl.sheet_names:
                    return pd.read_excel(fname, sheet_name=sname)
            except Exception:
                continue
    # 폴더 내 가장 최신 파일 자동 탐색 (패턴 일치하는 것 중)
    file_list = glob.glob(file_pattern.replace('%Y%m%d', '*'))
    if file_list:
        latest_file = max(file_list, key=os.path.getctime)
        xl = pd.ExcelFile(latest_file)
        for s in xl.sheet_names:
            if sheet_pattern.split('%Y%m%d')[0] in s:
                return pd.read_excel(latest_file, sheet_name=s)
    print(f"[Warn] 최근 {max_days}일간 {file_pattern} 또는 {sheet_pattern} 데이터 없음")
    return pd.DataFrame()
def authenticate_google_sheets(keyfile_path):
    try:
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(keyfile_path, scopes=scopes)
        client = gspread.authorize(creds)
        print("Google Sheets 인증 성공!")
        return client
    except Exception as e:
        print(f"Google Sheets 인증 오류: {e}")
        return None

def upload_df_to_gsheet(gspread_client, sheet_id, worksheet_name, df):
    if gspread_client is None or df.empty:
        print(f"[Google Sheets] '{worksheet_name}' 업로드 스킵(클라이언트 없음 or 빈 데이터)")
        return
    try:
        spreadsheet = gspread_client.open_by_key(sheet_id)
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1", cols="1")
        worksheet.clear()
        df_to_upload = df.astype(object).where(pd.notnull(df), None)
        set_with_dataframe(worksheet, df_to_upload, include_index=False, resize=True)
        print(f"[Google Sheets] '{worksheet_name}' 업로드 완료 ({len(df)} rows)")
    except Exception as e:
        print(f"Google Sheets 업로드 오류: {e}")

# ======== 데이터 크롤러 (생략: 기존과 동일) ========
# ... get_expiry_summary / get_historical_prices / get_detailed_option_chains ...
# (코드 너무 길어, 본문에서 동일 부분 그대로 복사 붙여넣기!)

def get_expiry_summary(driver, tickers):
    print("\n--- 만기일별 요약 정보 크롤링 시작 ---")
    df_total_expiry = []
    for ticker in tickers:
        print(f"[{ticker}] 크롤링 중...")
        url = f"https://optioncharts.io/options/{ticker}"
        try:
            driver.get(url)
            table_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'optioncharts-table-styling')]"))
            )
            html = table_element.get_attribute("outerHTML")
            dfs = pd.read_html(StringIO(html))
            if dfs:
                df = dfs[0]
                df = flatten_columns(df)
                df['Ticker'] = ticker
                df_total_expiry.append(df)
                print(f"  -> {ticker}: {len(df)}개 row 저장")
            else:
                print(f"  -> {ticker}: 테이블을 찾았으나 데이터가 없음.")
        except Exception as e:
            print(f"  -> {ticker} 에서 표를 찾지 못했거나 오류 발생: {e}")
            continue

    if df_total_expiry:
        df_all = pd.concat(df_total_expiry, ignore_index=True)
        print(f"--- 만기일별 요약 정보 크롤링 완료: 총 {len(df_all)}행 ---")
        return df_all
    else:
        print("--- 만기일별 요약 정보 데이터 없음 ---")
        return pd.DataFrame()

# --- 주가 데이터 다운로드 ---
def get_historical_prices(tickers, period, tickers_to_exclude):
    print("\n--- 주가 데이터 다운로드 시작 ---")
    all_data = []
    tickers_for_download = [t for t in tickers if t not in tickers_to_exclude]

    for ticker in tickers_for_download:
        print(f"[{ticker}] 데이터 다운로드 중 (기간: {period})...")
        try:
            df = yf.download(ticker, period=period, progress=False, auto_adjust=True)
            if df.empty:
                print(f"  -> {ticker}: 데이터 없음, 건너뜀")
                continue
            df['Ticker'] = ticker
            df['MA20'] = df['Close'].rolling(window=20, min_periods=1).mean()
            df['ATR14_simple'] = (df['High'] - df['Low']).rolling(window=14, min_periods=1).mean()
            df_reset = df.reset_index()
            if 'Datetime' in df_reset.columns and 'Date' not in df_reset.columns:
                df_reset = df_reset.rename(columns={'Datetime': 'Date'})
            all_data.append(df_reset)
            print(f"  -> {ticker}: 데이터 다운로드 완료")
        except Exception as e:
            print(f"  -> {ticker} 에서 주가 데이터 다운로드 오류: {e}")
            continue

    if tickers_to_exclude:
        print(f"참고: 다음 티커는 주가 데이터 수집에서 제외되었습니다: {tickers_to_exclude}")

    if all_data:
        df_total = pd.concat(all_data, ignore_index=True)
        if 'Date' in df_total.columns:
            df_total['Date'] = pd.to_datetime(df_total['Date']).dt.strftime('%Y-%m-%d')
        print(f"--- 주가 데이터 다운로드 완료: 총 {len(df_total)}행 ---")
        return df_total
    else:
        print("--- 주가 데이터 없음 ---")
        return pd.DataFrame()

# --- 상세 옵션 체인 크롤링 ---
def get_detailed_option_chains(driver, tickers, base_url_template, max_expiry_days):
    print("\n--- 상세 옵션 체인 크롤링 시작 ---")
    all_results = []
    today = datetime.date.today()
    max_expiry_date = today + datetime.timedelta(days=max_expiry_days)

    for ticker_idx, ticker in enumerate(tickers):
        print(f"[{ticker_idx+1}/{len(tickers)}] {ticker} 옵션 체인 크롤링 중...")
        url = base_url_template.format(ticker=ticker)
        expiry_links_for_ticker = []
        try:
            driver.get(url)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, "//table[contains(@class,'optioncharts-table-styling')]//tr[@onclick]"))
            )
            rows = driver.find_elements(By.XPATH, "//table[contains(@class,'optioncharts-table-styling')]//tr[@onclick]")
            for r in rows:
                js_onclick = r.get_attribute('onclick')
                if js_onclick and 'window.location' in js_onclick:
                    match_partial = re.search(r"window\.location='(.*?)'", js_onclick)
                    if match_partial:
                        partial_link = match_partial.group(1)
                        m = re.search(r'expiration_dates=(\d{4}-\d{2}-\d{2})', partial_link)
                        if m:
                            expiry_str = m.group(1)
                            try:
                                expiry_dt = datetime.datetime.strptime(expiry_str, "%Y-%m-%d").date()
                                if today <= expiry_dt <= max_expiry_date:
                                    expiry_links_for_ticker.append("https://optioncharts.io" + partial_link)
                            except ValueError:
                                print(f"  -> {ticker}: 잘못된 날짜 형식 건너뜀 - {expiry_str}")
                                continue
            expiry_links_for_ticker = list(set(expiry_links_for_ticker))
            print(f"  -> {ticker}: {len(expiry_links_for_ticker)}개의 유효한 만기일 링크 발견 (최대 {max_expiry_days}일 이내)")
        except Exception as e:
            print(f"  -> {ticker} 에서 만기일 링크 추출 중 오류: {e}")
            continue

        for i, expiry_url in enumerate(expiry_links_for_ticker):
            print(f"    ({i+1}/{len(expiry_links_for_ticker)}) {ticker} 만기 옵션체인 가져오는 중: {expiry_url.split('?')[1] if '?' in expiry_url else expiry_url}")
            retries = 2
            for attempt in range(retries):
                try:
                    driver.get(expiry_url)
                    table_element = WebDriverWait(driver, 20).until(
                        EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'table-hover')]"))
                    )
                    html = table_element.get_attribute('outerHTML')
                    dfs = pd.read_html(StringIO(html), header=[0,1])
                    if not dfs:
                        print(f"    -> {ticker} 옵션체인 테이블 파싱 실패 (데이터 없음): {expiry_url.split('?')[1] if '?' in expiry_url else expiry_url}")
                        break
                    df = dfs[0].copy()
                    df = flatten_columns(df)
                    df['Ticker'] = ticker
                    df['Expiry_URL_Param'] = expiry_url.split('?')[1] if '?' in expiry_url else expiry_url
                    m = re.search(r'expiration_dates=(\d{4}-\d{2}-\d{2})', expiry_url)
                    if m:
                        df['Expiry_Date'] = m.group(1)
                    for col in df.columns:
                        if any(keyword in col for keyword in ['IV', 'Volume', 'Open Int', 'OI', 'Last', 'Bid', 'Ask', 'Strike', 'Price', 'Change']):
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    all_results.append(df)
                    break
                except Exception as e:
                    print(f"    -> {ticker} 옵션체인 테이블 처리 중 오류 (시도 {attempt+1}/{retries}) ({expiry_url.split('?')[1] if '?' in expiry_url else expiry_url}): {e}")
                    if attempt < retries - 1:
                        print("       잠시 후 재시도합니다...")
                        time.sleep(random.uniform(2,4))
        time.sleep(random.uniform(0.5, 1.5))

    if all_results:
        result_df = pd.concat(all_results, ignore_index=True)
        # Strike 컬럼 자동 감지
        strike_col_name = None
        cols_lower = [str(col).lower() for col in result_df.columns]
        strike_candidates = []
        for i, col_l in enumerate(cols_lower):
            if col_l == 'strike' or col_l.endswith('_strike') or col_l.startswith('strike_') or re.match(r'col\d+_strike', col_l):
                strike_candidates.append(result_df.columns[i])
        if strike_candidates:
            strike_col_name = strike_candidates[0]
        if strike_col_name and 'Ticker' in result_df.columns and 'Expiry_Date' in result_df.columns:
            print(f"   (중복 제거 기준 Strike 컬럼: {strike_col_name})")
            try:
                result_df = result_df.drop_duplicates(subset=['Ticker', 'Expiry_Date', strike_col_name], keep='first')
                print(f"--- 상세 옵션 체인 크롤링 완료 (중복 제거 후): 총 {len(result_df)}행 ---")
            except KeyError as ke:
                print(f"    중복 제거 중 KeyError 발생 (컬럼 확인 필요: {ke}). 중복 제거 스킵.")
                print(f"--- 상세 옵션 체인 크롤링 완료 (중복 제거 실패): 총 {len(result_df)}행 ---")
        else:
            print(f"--- 상세 옵션 체인 크롤링 완료 (Strike 컬럼 자동 감지 실패 또는 필요 컬럼 부재로 중복 제거 스킵): 총 {len(result_df)}행 ---")
            if not strike_col_name: print("    주의: Strike 컬럼명을 자동으로 찾지 못했습니다. 컬럼명 확인 필요.")
        return result_df
    else:
        print("--- 상세 옵션 체인 데이터 없음 ---")
        return pd.DataFrame()

# ... 기타 필요한 import (gspread, excel 로딩 함수 등) ...
#======================================================시그널 최신 날짜 찾기
def get_latest_prev_file(today_str, prefix='market_data_report_', suffix='.xlsx'):
    files = glob.glob(f"{prefix}*.xlsx")
    date_pat = re.compile(rf"{prefix}(\d{{8}}){suffix}")
    prev_files = []
    for f in files:
        m = date_pat.match(os.path.basename(f))
        if m:
            file_date = m.group(1)
            if file_date < today_str:
                prev_files.append(file_date)
    if not prev_files:
        return None, None
    prev_files.sort(reverse=True)
    prev_str = prev_files[0]
    file_prev = f"{prefix}{prev_str}{suffix}"
    return file_prev, prev_str

def main_signals_and_upload():
    today_str = datetime.datetime.now().strftime('%Y%m%d')
    file_today = f"market_data_report_{today_str}.xlsx"
    file_prev, prev_str = get_latest_prev_file(today_str)

    sheet_today = f"OptionChains_Detail_{today_str}"
    expiry_today = f"ExpirySummary_{today_str}"

    df_today = safe_load_excel(file_today, sheet_today)
    df_expiry = safe_load_excel(file_today, expiry_today)

    if file_prev and prev_str:
        sheet_prev = f"OptionChains_Detail_{prev_str}"
        df_prev = safe_load_excel(file_prev, sheet_prev)
    else:
        df_prev = None

    def fix_strike_column(df):
        for col in df.columns:
            if 'strike' in col.lower() and col != 'Strike':
                df = df.rename(columns={col: 'Strike'})
        return df


    df_today = fix_strike_column(df_today)
    if df_prev is not None:
        df_prev = fix_strike_column(df_prev)
    # 이제 여기서 df_prev가 None이면 오늘 데이터만 필요한 시그널만 추출하도록 분기!
    if df_today is None or df_today.empty:
        print("오늘 입력 데이터가 부족합니다.")
        return

    if df_prev is None or df_prev.empty:
        print("이전 데이터가 없어 오늘 데이터만 생성 가능한 시그널만 추출합니다.")
        # df_prev 없이 가능한 시그널만 생성 (예시: PCR, Max Pain, ATM 집중 등)
        # ... (이하 오늘 데이터만 활용한 시그널 코드)
        # 예시:
        pcr_signals = extract_pcr_signals(df_today)
        if not pcr_signals.empty:
            pcr_signals.to_excel(f"Option_PCR_Signal_{today_str}.xlsx", index=False)
        # ... 이하 생략
        return

    
# ======== 시그널 추출/저장/구글시트 업로드 (함수명 일치 리뉴얼) ========
#===================================================================
    # 1. 콜/풋 OI·볼륨 급증 신호
    call_signal = extract_oi_volume_signal(df_today, df_prev, kind='Calls')
    put_signal = extract_oi_volume_signal(df_today, df_prev, kind='Puts')
    if not call_signal.empty:
        call_signal.to_excel(f"Option_Call_OI_Signal_{today_str}.xlsx", index=False)
    if not put_signal.empty:
        put_signal.to_excel(f"Option_Put_OI_Signal_{today_str}.xlsx", index=False)

  # 2. IV 급등락 신호 (IV 컬럼이 없으면 자동 생략)
    if 'Calls_IV' in df_today.columns and 'Calls_IV' in df_prev.columns:
        iv_jump_call, iv_drop_call = extract_iv_signal(df_today, df_prev, kind='Calls')
        if not iv_jump_call.empty:
            iv_jump_call.to_excel(f"Option_Call_IV_Jump_{today_str}.xlsx", index=False)
        if not iv_drop_call.empty:
            iv_drop_call.to_excel(f"Option_Call_IV_Drop_{today_str}.xlsx", index=False)
    else:
        print("[SKIP] Calls_IV 컬럼 없음: IV Jump/Drop 신호 생략")

    if 'Puts_IV' in df_today.columns and 'Puts_IV' in df_prev.columns:
        iv_jump_put, iv_drop_put = extract_iv_signal(df_today, df_prev, kind='Puts')
        if not iv_jump_put.empty:
            iv_jump_put.to_excel(f"Option_Put_IV_Jump_{today_str}.xlsx", index=False)
        if not iv_drop_put.empty:
            iv_drop_put.to_excel(f"Option_Put_IV_Drop_{today_str}.xlsx", index=False)
    else:
        print("[SKIP] Puts_IV 컬럼 없음: IV Jump/Drop 신호 생략")
    # 3. 프로 스타일 OI 변화(%) + 변화량 + 볼륨
    oi_signals = extract_oi_signals(df_today, df_prev)
    if not oi_signals.empty:
        oi_signals.to_excel(f"Option_OI_ProSignal_{today_str}.xlsx", index=False)

    # 4. PCR (Put/Call Ratio) 신호
    pcr_signals = extract_pcr_signals(df_today)
    if not pcr_signals.empty:
        pcr_signals.to_excel(f"Option_PCR_Signal_{today_str}.xlsx", index=False)

    # 5. Max Pain 신호
    if df_expiry is not None and not df_expiry.empty:
        import yfinance as yf

        if 'Ticker' in df_expiry.columns:
            tickers = df_expiry['Ticker'].unique().tolist()
            prices = {}
            for ticker in tickers:
                try:
                    tkr = yf.Ticker(ticker)
                    price = tkr.history(period='1d')['Close'].iloc[-1]
                    prices[ticker] = price
                except Exception as e:
                    print(f"{ticker} 현재가 추출 실패: {e}")
                    prices[ticker] = None
            df_expiry['Current Price'] = df_expiry['Ticker'].map(prices)

        if 'Current Price' in df_expiry.columns and 'Max Pain_Max Pain' in df_expiry.columns:
            max_pain_signals = extract_max_pain_signals(
                df_expiry,
                current_price_col='Current Price',
                max_pain_col='Max Pain_Max Pain'
            )
            if not max_pain_signals.empty:
                max_pain_signals.to_excel(f"Option_MaxPain_Signal_{today_str}.xlsx", index=False)
        else:
            print("[SKIP] ExpirySummary에 Current Price 또는 Max Pain 컬럼 없음: Max Pain 신호 생략")

    # 6. ATM 집중거래 신호
    atm_call_focus = extract_atm_focus_signal(df_today, option_type='call')
    atm_put_focus = extract_atm_focus_signal(df_today, option_type='put', volume_col='Puts_Volume', oi_col='Puts_Open Interest')
    if not atm_call_focus.empty:
        atm_call_focus.to_excel(f"Option_ATM_Call_Focus_{today_str}.xlsx", index=False)
    if not atm_put_focus.empty:
        atm_put_focus.to_excel(f"Option_ATM_Put_Focus_{today_str}.xlsx", index=False)

    # 7. 롤오버/청산(만기임박 OI 변화) 신호 (티커별 루프)
    all_rollover_signals = []
    for ticker in df_today['Ticker'].unique():
        sigs = detect_rollover_and_unwind_signals(df_today, df_prev, ticker)
        if not sigs.empty:
            all_rollover_signals.append(sigs)
    if all_rollover_signals:
        pd.concat(all_rollover_signals, ignore_index=True).to_excel(f"Option_Rollover_Signals_{today_str}.xlsx", index=False)

    # 8. 감마/델타 스퀴즈
    spot_price = None
    if 'Underlying Price' in df_today.columns:
        spot_price = df_today['Underlying Price'].mode().iloc[0]
    if spot_price is not None:
        gamma_squeeze = detect_gamma_squeeze(df_today, spot_price)
        if not gamma_squeeze.empty:
            gamma_squeeze.to_excel(f"Option_Gamma_Squeeze_{today_str}.xlsx", index=False)

    print("=== 모든 시그널 추출/저장 완료 ===")


# ======== 메인 시작 ======================================
if __name__ == "__main__":
    # 1. 크롤링/엑셀저장
    main_crawler()
    # 2. 시그널 추출/저장
    main_signals_and_upload()
    import time
    time.sleep(1)
    # 3. 구글시트 업로드
    upload_to_google_sheets()