# signals.py

import os
import pandas as pd
import datetime

# 파일/시트 예외처리
def safe_load_excel(file_path, sheet_name):
    if not os.path.exists(file_path):
        print(f"[경고] 파일 없음: {file_path}")
        return None
    try:
        return pd.read_excel(file_path, sheet_name=sheet_name)
    except Exception as e:
        print(f"[경고] {file_path} → {sheet_name} 읽기 실패: {e}")
        return None

# ---- 아래는 시그널 함수들 ----

# 1. 콜/풋 OI·볼륨 급증 신호
def extract_oi_volume_signal(df_today, df_prev, kind='Calls', oi_threshold_pct=20, volume_threshold=500):
    merge_cols = ['Ticker', 'Expiry_Date', 'Strike']
    col_oi = f'{kind}_Open Interest'
    col_vol = f'{kind}_Volume'
    df_merge = pd.merge(df_today, df_prev, on=merge_cols, suffixes=('', '_prev'), how='left')
    df_merge[f'{kind}_OI_Change_Pct'] = (
        (df_merge[col_oi] - df_merge[f"{col_oi}_prev"]) / df_merge[f"{col_oi}_prev"].replace(0, 1e-6)
    ) * 100
    signal = df_merge[
        (df_merge[f'{kind}_OI_Change_Pct'] > oi_threshold_pct) &
        (df_merge[col_vol] > volume_threshold)
    ].copy()
    return signal

# 2. IV 급등락 신호
def extract_iv_signal(df_today, df_prev, kind='Calls', iv_jump_threshold=20):
    merge_cols = ['Ticker', 'Expiry_Date', 'Strike']
    col_iv = f'{kind}_IV'
    df_merge = pd.merge(df_today, df_prev, on=merge_cols, suffixes=('', '_prev'), how='left')
    df_merge[f'{kind}_IV_Change_Pct'] = (
        (df_merge[col_iv] - df_merge[f"{col_iv}_prev"]) / df_merge[f"{col_iv}_prev"].replace(0, 1e-6)
    ) * 100
    iv_jump_signal = df_merge[df_merge[f'{kind}_IV_Change_Pct'] > iv_jump_threshold].copy()
    iv_drop_signal = df_merge[df_merge[f'{kind}_IV_Change_Pct'] < -iv_jump_threshold].copy()
    return iv_jump_signal, iv_drop_signal

# 3. 프로 스타일 OI 변화(%) + 변화량 + 볼륨
def extract_oi_signals(
    df_today, df_prev,
    min_oi_change_pct=20,
    min_oi_change_amt=500,
    min_volume=1000,
    ignore_near_expiry=3
):
    if ignore_near_expiry > 0 and 'Expiry_Date' in df_today.columns:
        today_date = pd.to_datetime(datetime.datetime.now().date())
        df_today['Expiry_Date_dt'] = pd.to_datetime(df_today['Expiry_Date'])
        df_today = df_today[(df_today['Expiry_Date_dt'] - today_date).dt.days >= ignore_near_expiry]
        df_prev['Expiry_Date_dt'] = pd.to_datetime(df_prev['Expiry_Date'])
        df_prev = df_prev[(df_prev['Expiry_Date_dt'] - today_date).dt.days >= ignore_near_expiry]
    merge_cols = ['Ticker', 'Expiry_Date', 'Strike']
    df_merge = pd.merge(
        df_today, df_prev,
        on=merge_cols, suffixes=('', '_prev'), how='left'
    )
    signals = []
    for typ in ['Call', 'Put']:
        oi_col = f'{typ}s_Open Interest'
        vol_col = f'{typ}s_Volume'
        oi_col_prev = f'{oi_col}_prev'
        df_merge[f'{typ}_OI_Change_Pct'] = (
            (df_merge[oi_col] - df_merge[oi_col_prev]) / df_merge[oi_col_prev].replace(0, 1e-6)
        ) * 100
        df_merge[f'{typ}_OI_Change_Amt'] = df_merge[oi_col] - df_merge[oi_col_prev]
        sig = df_merge[
            (df_merge[f'{typ}_OI_Change_Pct'] > min_oi_change_pct) &
            (df_merge[f'{typ}_OI_Change_Amt'] > min_oi_change_amt) &
            (df_merge[vol_col] > min_volume)
        ].copy()
        sig['Signal_Type'] = f'{typ} OI/Volume Surge'
        signals.append(sig)
    result = pd.concat(signals, ignore_index=True) if signals else pd.DataFrame()
    return result

# 4. PCR (Put/Call Ratio) 신호
def extract_pcr_signals(
    df,
    min_total_oi=1000,
    min_total_vol=500,
    pcr_oi_high=2.0,
    pcr_oi_low=0.5,
    pcr_vol_high=2.0,
    pcr_vol_low=0.5
):
    df = df.copy()
    df['PCR_OI'] = df['Puts_Open Interest'] / df['Calls_Open Interest'].replace(0, 1e-6)
    df['PCR_VOL'] = df['Puts_Volume'] / df['Calls_Volume'].replace(0, 1e-6)
    df = df[
        ((df['Puts_Open Interest'] + df['Calls_Open Interest']) > min_total_oi) &
        ((df['Puts_Volume'] + df['Calls_Volume']) > min_total_vol)
    ].copy()
    pcr_oi_high_df = df[df['PCR_OI'] > pcr_oi_high].copy()
    pcr_oi_high_df['Signal_Type'] = f'PCR_OI > {pcr_oi_high} (극단 하방)'
    pcr_oi_low_df = df[df['PCR_OI'] < pcr_oi_low].copy()
    pcr_oi_low_df['Signal_Type'] = f'PCR_OI < {pcr_oi_low} (극단 상방)'
    pcr_vol_high_df = df[df['PCR_VOL'] > pcr_vol_high].copy()
    pcr_vol_high_df['Signal_Type'] = f'PCR_VOL > {pcr_vol_high} (풋매수 쏠림)'
    pcr_vol_low_df = df[df['PCR_VOL'] < pcr_vol_low].copy()
    pcr_vol_low_df['Signal_Type'] = f'PCR_VOL < {pcr_vol_low} (콜매수 쏠림)'
    result = pd.concat([pcr_oi_high_df, pcr_oi_low_df, pcr_vol_high_df, pcr_vol_low_df], ignore_index=True)
    return result

# 5. Max Pain 신호
def extract_max_pain_signals(
    df_expiry,
    current_price_col='Current Price',
    max_pain_col='Max Pain',
    min_oi=1000,
    gap_pct=5.0
):
    df = df_expiry.copy()
    for col in [current_price_col, max_pain_col]:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['MaxPain_Gap_Pct'] = ((df[current_price_col] - df[max_pain_col]) / df[max_pain_col].replace(0, 1e-6)) * 100
    if 'Total OI' in df.columns:
        df = df[df['Total OI'] > min_oi]
    pos_gap = df[df['MaxPain_Gap_Pct'] >= gap_pct].copy()
    pos_gap['Signal_Type'] = f'현물 ↑ Max Pain +{gap_pct}% 이상 (상단괴리)'
    neg_gap = df[df['MaxPain_Gap_Pct'] <= -gap_pct].copy()
    neg_gap['Signal_Type'] = f'현물 ↓ Max Pain -{gap_pct}% 이상 (하단괴리)'
    result = pd.concat([pos_gap, neg_gap], ignore_index=True)
    return result

# 6. ATM 집중거래 신호
def extract_atm_focus_signal(
    df_chain,
    current_price_col='Underlying Price',
    strike_col='Strike',
    volume_col='Calls_Volume',
    oi_col='Calls_Open Interest',
    threshold_ratio=0.4,
    option_type='call',
    min_total_vol=1000
):
    result = []
    for (ticker, expiry), group in df_chain.groupby(['Ticker', 'Expiry_Date']):
        underlying = group[current_price_col].iloc[0] if current_price_col in group.columns else None
        if underlying is None:
            continue
        strikes = group[strike_col].dropna().unique()
        if len(strikes) < 3:
            continue
        atm_strike = min(strikes, key=lambda x: abs(x - underlying))
        strikes_sorted = sorted(strikes)
        try:
            atm_idx = strikes_sorted.index(atm_strike)
        except:
            continue
        atm_window_idx = [i for i in [atm_idx-1, atm_idx, atm_idx+1] if 0 <= i < len(strikes_sorted)]
        atm_window = [strikes_sorted[i] for i in atm_window_idx]
        vol = group[volume_col].sum()
        oi = group[oi_col].sum()
        atm_vol = group[group[strike_col].isin(atm_window)][volume_col].sum()
        atm_oi = group[group[strike_col].isin(atm_window)][oi_col].sum()
        if vol >= min_total_vol:
            vol_ratio = atm_vol / (vol + 1e-6)
            if vol_ratio >= threshold_ratio:
                result.append({
                    'Ticker': ticker,
                    'Expiry_Date': expiry,
                    'Type': f'ATM_{option_type.upper()}_Volume_Focus',
                    'Underlying': underlying,
                    'ATM_Strike': atm_strike,
                    'ATM_Window': atm_window,
                    'ATM_Window_Volume': atm_vol,
                    'Total_Volume': vol,
                    'ATM_Volume_Ratio': vol_ratio,
                    'Comment': f"{option_type.upper()} ATM±1 거래집중({vol_ratio:.0%})"
                })
        if oi >= min_total_vol:
            oi_ratio = atm_oi / (oi + 1e-6)
            if oi_ratio >= threshold_ratio:
                result.append({
                    'Ticker': ticker,
                    'Expiry_Date': expiry,
                    'Type': f'ATM_{option_type.upper()}_OI_Focus',
                    'Underlying': underlying,
                    'ATM_Strike': atm_strike,
                    'ATM_Window': atm_window,
                    'ATM_Window_OI': atm_oi,
                    'Total_OI': oi,
                    'ATM_OI_Ratio': oi_ratio,
                    'Comment': f"{option_type.upper()} ATM±1 미결집중({oi_ratio:.0%})"
                })
    return pd.DataFrame(result)

# 7. 롤오버/청산(만기임박 OI 변화)
def detect_rollover_and_unwind_signals(
    df_today, df_prev, ticker,
    expiry_col='Expiry_Date',
    oi_col='Calls_Open Interest',
    min_days_to_expiry=0,
    max_days_to_expiry=7,
    change_pct_threshold=0.3,
    abs_change_threshold=2000,
    volume_col='Calls_Volume',
    min_volume=500
):
    signals = []
    today = datetime.datetime.now().date()
    df_today_tk = df_today[df_today['Ticker']==ticker].copy()
    df_prev_tk  = df_prev[df_prev['Ticker']==ticker].copy()
    df_today_tk[expiry_col] = pd.to_datetime(df_today_tk[expiry_col]).dt.date
    df_prev_tk[expiry_col] = pd.to_datetime(df_prev_tk[expiry_col]).dt.date
    mask_expiry = df_today_tk[expiry_col].apply(
        lambda x: min_days_to_expiry <= (x - today).days <= max_days_to_expiry
    )
    df_today_tk = df_today_tk[mask_expiry]
    df_prev_tk = df_prev_tk[df_prev_tk[expiry_col].isin(df_today_tk[expiry_col].unique())]
    oi_today = df_today_tk.groupby(expiry_col)[oi_col].sum().reset_index(name='OI_today')
    oi_prev  = df_prev_tk.groupby(expiry_col)[oi_col].sum().reset_index(name='OI_prev')
    vol_today = df_today_tk.groupby(expiry_col)[volume_col].sum().reset_index(name='Vol_today')
    df_oi = pd.merge(oi_today, oi_prev, on=expiry_col, how='outer').fillna(0)
    df_oi = pd.merge(df_oi, vol_today, on=expiry_col, how='left').fillna(0)
    df_oi['OI_Change'] = df_oi['OI_today'] - df_oi['OI_prev']
    df_oi['OI_Change_Pct'] = (df_oi['OI_Change'] / (df_oi['OI_prev'].replace(0, 1e-6))) * 100
    unwind = df_oi[
        (df_oi['OI_prev'] > 0) &
        (df_oi['OI_Change'] < 0) &
        ((abs(df_oi['OI_Change']) > abs_change_threshold) |
         (abs(df_oi['OI_Change_Pct']) > change_pct_threshold*100)) &
        (df_oi['Vol_today'] > min_volume)
    ]
    for _, row in unwind.iterrows():
        signals.append({
            'Ticker': ticker,
            'Expiry': row[expiry_col],
            'Type': '청산(언와인드)',
            'OI_prev': row['OI_prev'],
            'OI_today': row['OI_today'],
            'OI_Change': row['OI_Change'],
            'OI_Change_Pct': row['OI_Change_Pct'],
            'Vol_today': row['Vol_today'],
            'Comment': '만기 임박 대량 청산(포지션 정리) 감지'
        })
    rollover = df_oi[
        (df_oi['OI_Change'] > abs_change_threshold) |
        (df_oi['OI_Change_Pct'] > change_pct_threshold*100)
    ]
    for _, row in rollover.iterrows():
        days_left = (row[expiry_col] - today).days
        if max_days_to_expiry < days_left <= max_days_to_expiry+23 and row['OI_today'] > 0:
            signals.append({
                'Ticker': ticker,
                'Expiry': row[expiry_col],
                'Type': '롤오버(차월로 이동)',
                'OI_prev': row['OI_prev'],
                'OI_today': row['OI_today'],
                'OI_Change': row['OI_Change'],
                'OI_Change_Pct': row['OI_Change_Pct'],
                'Vol_today': row['Vol_today'],
                'Comment': '차월 대량 롤오버(포지션 이동) 감지'
            })
    return pd.DataFrame(signals)

# 8. 감마/델타 스퀴즈 (감마 집중 스트라이크)
def detect_gamma_squeeze(
    df, spot_price,
    gamma_col='Calls_Gamma',
    oi_col='Calls_Open Interest',
    strike_col='Strike',
    put_gamma_col='Puts_Gamma',
    put_oi_col='Puts_Open Interest',
    near_pct=0.02,
    gamma_focus_pct=0.15,
    min_abs_gamma=0.02
):
    result = []
    df['Call_Gamma_Exposure'] = df[gamma_col] * df[oi_col]
    df['Put_Gamma_Exposure'] = df[put_gamma_col] * df[put_oi_col]
    df['Total_Gamma_Exposure'] = df['Call_Gamma_Exposure'] + df['Put_Gamma_Exposure']
    total_gamma_sum = df['Total_Gamma_Exposure'].abs().sum()
    for idx, row in df.iterrows():
        strike = row[strike_col]
        abs_dist = abs(strike - spot_price) / spot_price
        gamma_exp = row['Total_Gamma_Exposure']
        if (abs_dist < near_pct and
            abs(gamma_exp) > gamma_focus_pct * total_gamma_sum and
            abs(gamma_exp) > min_abs_gamma):
            result.append({
                'Strike': strike,
                'Gamma_Exposure': gamma_exp,
                'Abs_Dist_Spot': abs_dist,
                'Gamma_Ratio': gamma_exp/total_gamma_sum,
                'Comment': '감마 쏠림/감마 스퀴즈 구간'
            })
    return pd.DataFrame(result)
