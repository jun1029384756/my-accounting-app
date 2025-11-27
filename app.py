import streamlit as st
import pandas as pd
import plotly.express as px
import json
import os
import sqlite3
import re
from datetime import datetime

# ==========================================
# 1. 後端邏輯區
# ==========================================

DB_NAME = "accounting.db"
CATEGORY_OPTIONS = [
    "飲食", "日常用品", "交通", "水電瓦斯", "居家", 
    "服飾", "娛樂", "美容美髮", "交際應酬", "學習深造", 
    "車", "醫療保健", "3C家電", "其他"
]

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS expenses 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, store TEXT, item TEXT, price INTEGER)''')
    c.execute("PRAGMA table_info(expenses)")
    columns = [info[1] for info in c.fetchall()]
    if "fixed_category" not in columns:
        c.execute("ALTER TABLE expenses ADD COLUMN fixed_category TEXT")
    conn.commit()
    conn.close()

def save_to_db(df):
    conn = sqlite3.connect(DB_NAME)
    # 確保有 fixed_category 欄位，如果沒有就補上 None
    if 'fixed_category' not in df.columns:
        df['fixed_category'] = None
        
    data = df[['日期', '商店名稱', '品項', '金額', 'fixed_category']].copy()
    data.columns = ['date', 'store', 'item', 'price', 'fixed_category']
    data.to_sql('expenses', conn, if_exists='append', index=False)
    conn.close()

def update_transaction(row_id, new_item_name, new_price, new_category):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("UPDATE expenses SET item = ?, price = ?, fixed_category = ? WHERE id = ?", 
              (new_item_name, new_price, new_category, row_id))
    conn.commit()
    conn.close()

def delete_transaction(row_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM expenses WHERE id = ?", (row_id,))
    conn.commit()
    conn.close()

def split_transaction(original_id, new_items_df):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        c.execute("DELETE FROM expenses WHERE id = ?", (original_id,))
        for index, row in new_items_df.iterrows():
            c.execute('''INSERT INTO expenses (date, store, item, price, fixed_category) 
                         VALUES (?, ?, ?, ?, ?)''', 
                         (row['date'], row['store'], row['item'], row['price'], row['fixed_category']))
        conn.commit()
        return True
    except: return False
    finally: conn.close()

def load_from_db():
    conn = sqlite3.connect(DB_NAME)
    try:
        df = pd.read_sql("SELECT id, date, store, item, price, fixed_category FROM expenses", conn)
        df = df.rename(columns={'date': '日期', 'store': '商店名稱', 'item': '品項', 'price': '金額'})
        df['日期'] = pd.to_datetime(df['日期'])
        df['月份'] = df['日期'].dt.strftime('%Y-%m')
    except: df = pd.DataFrame()
    conn.close()
    return df

def clear_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM expenses")
    conn.commit()
    conn.close()

def load_custom_rules():
    if os.path.exists('rules.json'):
        with open('rules.json', 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
            normalized = {}
            for k, v in raw_data.items():
                if isinstance(v, str): normalized[k] = {"category": v, "item": None}
                else: normalized[k] = v
            return normalized
    return {}

def save_custom_rule(keyword, category, item_name=None):
    rules = load_custom_rules()
    rules[keyword] = {"category": category, "item": item_name}
    with open('rules.json', 'w', encoding='utf-8') as f: json.dump(rules, f, ensure_ascii=False, indent=4)

def save_all_rules(new_rules_dict):
    with open('rules.json', 'w', encoding='utf-8') as f: json.dump(new_rules_dict, f, ensure_ascii=False, indent=4)

def apply_rules_to_row(row, custom_rules):
    final_cat = "其他"
    final_item = str(row.get('品項', ''))
    s = str(row.get('商店名稱', ''))
    
    if pd.notna(row.get('fixed_category')) and row.get('fixed_category'):
        final_cat = row['fixed_category']
        return pd.Series([final_cat, final_item])

    rule_matched = False
    for k, v in custom_rules.items():
        if k in s or k in final_item:
            final_cat = v['category']
            if v.get('item') and final_item in ["一般消費", "-", "nan", ""]:
                final_item = v['item']
            rule_matched = True
            break
    
    if not rule_matched:
        if "7-ELEVEN" in s or "全家" in s: final_cat = "飲食"
        elif "全聯" in s or "家樂福" in s: final_cat = "日常用品"
        elif "中油" in s: final_cat = "車"
        elif "Uber" in s or "高鐵" in s or "台鐵" in s: final_cat = "交通"
        elif "星巴克" in s or "麥當勞" in s or "壽司郎" in s: final_cat = "飲食"
        elif "Uniqlo" in s or "NET" in s: final_cat = "服飾"
        elif "屈臣氏" in s or "康是美" in s: final_cat = "醫療保健"
        elif "好市多" in s or "Costco" in s: final_cat = "日常用品"
    
    return pd.Series([final_cat, final_item])

def parse_messy_excel(df_raw):
    clean_data = []
    all_rows = []
    for index, row in df_raw.iterrows():
        row_str = " ".join([str(x) for x in row.values if pd.notna(x)])
        all_rows.append(row_str)

    temp_date, temp_price = None, None
    for line in all_rows:
        date_match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', line)
        if date_match:
            year, month, day = date_match.groups()
            temp_date = f"{year}-{int(month):02d}-{int(day):02d}"
            numbers = re.findall(r'\b\d+\b', line)
            for num in numbers:
                val = int(num)
                if val != int(year) and val != int(month) and val != int(day) and val < 1000000:
                    temp_price = val
        elif temp_date and temp_price:
            store_name = line.strip()
            if store_name and "變條碼" not in store_name:
                clean_data.append({"日期": temp_date, "商店名稱": store_name, "品項": "一般消費", "金額": temp_price})
                temp_date, temp_price = None, None
    return pd.DataFrame(clean_data)

# --- 🔥 新增：天天記帳 App 專用解析器 ---
def parse_daily_accounting(df):
    """處理天天記帳匯出的 CSV"""
    # 1. 篩選只要「支出」
    if '收支區分' in df.columns:
        df = df[df['收支區分'] == '支'].copy()
    
    # 2. 處理日期 (20251126 -> 2025-11-26)
    # 先轉字串，再轉日期格式
    df['日期'] = pd.to_datetime(df['日期'].astype(str), format='%Y%m%d').dt.strftime('%Y-%m-%d')
    
    # 3. 處理品項 (使用備註，若無則用空白)
    df['品項'] = df['備註'].fillna('')
    # 如果備註是空的，就用類別名稱代替 (例如「飲食」)
    df.loc[df['品項'] == '', '品項'] = df['類別']
    
    # 4. 處理商店名稱 (天天記帳通常沒有店名，給預設值)
    df['商店名稱'] = '-' 
    
    # 5. 處理分類 (直接沿用 App 的分類到 fixed_category)
    # 這裡可以做一個簡單的對照，或者直接信賴 App 的分類
    df['fixed_category'] = df['類別'] # 這一招很強，直接把它的分類變成我們的「強制分類」
    
    return df[['日期', '商店名稱', '品項', '金額', 'fixed_category']]

# ==========================================
# 2. 前端介面區
# ==========================================

init_db()
st.set_page_config(page_title="My Asset | 智慧記帳", page_icon="💳", layout="wide")

st.sidebar.title("功能控制台")

import_mode = st.sidebar.radio("匯入模式", ["✍️ 手動輸入", "📂 上傳 Excel/CSV"], label_visibility="collapsed")
if 'preview_df' not in st.session_state: st.session_state.preview_df = None

if import_mode == "✍️ 手動輸入":
    with st.sidebar.form("manual"):
        m_date = st.date_input("日期")
        m_store = st.text_input("商店", placeholder="選填")
        m_item = st.text_input("品項", placeholder="必填")
        m_price = st.number_input("金額", min_value=0, value=100)
        if st.form_submit_button("新增"):
            s = m_store if m_store else "-"
            save_to_db(pd.DataFrame([{"日期": m_date.strftime("%Y-%m-%d"), "商店名稱": s, "品項": m_item, "金額": m_price}]))
            st.rerun()

elif import_mode == "📂 上傳 Excel/CSV":
    st.sidebar.caption("支援：財政部 Excel/CSV、天天記帳 CSV")
    up_file = st.sidebar.file_uploader("選擇檔案", type=["csv", "xlsx"])
    if up_file:
        try:
            # 1. 讀取檔案
            if up_file.name.endswith('.csv'):
                try: df_raw = pd.read_csv(up_file)
                except: 
                    up_file.seek(0)
                    df_raw = pd.read_csv(up_file, encoding='big5') # 嘗試 Big5 讀取 (天天記帳有時候需要)
            else: df_raw = pd.read_excel(up_file)
            
            # 2. 判斷格式並轉換
            if '收支區分' in df_raw.columns and '備註' in df_raw.columns:
                st.sidebar.success("偵測到「天天記帳」格式！")
                df_clean = parse_daily_accounting(df_raw)
                
            elif '商店名稱' not in df_raw.columns and '店名' not in df_raw.columns:
                st.sidebar.info("偵測到財政部複製貼上格式...")
                df_clean = parse_messy_excel(df_raw)
            else:
                st.sidebar.info("偵測到標準格式...")
                df_clean = df_raw.rename(columns={'消費日期':'日期', '店名':'商店名稱', '總金額':'金額'})
                if '品項' not in df_clean: df_clean['品項'] = '一般消費'
                
            if not df_clean.empty: st.session_state.preview_df = df_clean
            
        except Exception as e: st.sidebar.error(f"解析失敗：{e}")

if st.session_state.preview_df is not None:
    st.sidebar.success(f"成功辨識 {len(st.session_state.preview_df)} 筆！")
    st.sidebar.dataframe(st.session_state.preview_df.head(3), height=100)
    if st.sidebar.button("✅ 確認匯入資料庫"):
        save_to_db(st.session_state.preview_df)
        st.session_state.preview_df = None
        st.success("匯入完成！")
        st.rerun()

st.sidebar.divider()

# --- 月份篩選器 ---
df_all = load_from_db()
selected_month = "所有時間"

if not df_all.empty:
    month_list = sorted(df_all['月份'].unique(), reverse=True)
    month_options = ["所有時間"] + list(month_list)
    st.sidebar.subheader("📅 時間篩選")
    selected_month = st.sidebar.selectbox("選擇月份查看", month_options)
    if selected_month == "所有時間":
        df_display = df_all.copy()
    else:
        df_display = df_all[df_all['月份'] == selected_month].copy()
else:
    df_display = pd.DataFrame()

# --- 危險區域 ---
with st.sidebar.expander("🗑️ 危險區域 (清空資料)"):
    st.warning("注意：這會刪除所有帳務紀錄！")
    if st.button("確認清空所有資料"):
        clear_db()
        st.success("資料庫已清空")
        st.rerun()

# ==========================================
# 主畫面
# ==========================================
st.title("💳 My Asset 智慧記帳")

tab1, tab2, tab3, tab4 = st.tabs(["📊 月度分析 (Trends)", "📂 帳務明細 (刪除/編輯)", "⚙️ 規則管理", "✂️ 拆帳"])

if not df_all.empty:
    rules = load_custom_rules()
    df_all[['分類結果', '顯示品項']] = df_all.apply(lambda row: apply_rules_to_row(row, rules), axis=1)
    df_display = df_all.loc[df_display.index].copy()
    uk_count = len(df_display[df_display['分類結果']=='其他'])
    
    with tab1:
        st.subheader("📈 每月消費趨勢")
        trend_data = df_all.groupby('月份')['金額'].sum().reset_index()
        fig_trend = px.bar(trend_data, x='月份', y='金額', text='金額', color='月份')
        st.plotly_chart(fig_trend, use_container_width=True)
        st.divider()
        st.subheader(f"📊 {selected_month} 消費分析")
        c1, c2, c3 = st.columns(3)
        c1.metric("總消費", f"${df_display['金額'].sum():,}")
        c2.metric("總筆數", f"{len(df_display)}")
        c3.metric("未分類", f"{uk_count}", delta="需處理" if uk_count>0 else "OK", delta_color="inverse" if uk_count>0 else "off")
        
        col_l, col_r = st.columns(2)
        fig_pie = px.pie(df_display, values='金額', names='分類結果', hole=0.5, title="分類佔比")
        col_l.plotly_chart(fig_pie, use_container_width=True)
        
        bar_data = df_display.groupby('分類結果')['金額'].sum().reset_index().sort_values('金額', ascending=True)
        fig_bar = px.bar(bar_data, x='金額', y='分類結果', orientation='h', text='金額', title="分類排行 (點擊查看明細)")
        selected_event = col_r.plotly_chart(fig_bar, use_container_width=True, on_select="rerun", key="bar_select")
        
        if len(selected_event.selection.points) > 0:
            cat = selected_event.selection.points[0]['y']
            st.divider()
            st.subheader(f"📂 「{cat}」分類詳細明細")
            st.caption("點擊圖表空白處可取消篩選")
            filtered_df = df_display[df_display['分類結果'] == cat].sort_values('日期', ascending=False)
            st.dataframe(filtered_df[['日期', '商店名稱', '顯示品項', '金額']], use_container_width=True, column_config={"顯示品項": "品項"})

    with tab2:
        col_search, col_date = st.columns([1, 1])
        search_term = col_search.text_input("🔍 關鍵字搜尋", placeholder="例如：全家、咖啡、100")
        today = datetime.now()
        first_day = today.replace(day=1)
        date_range = col_date.date_input("📅 日期範圍篩選", value=(first_day, today))
        
        df_editor = df_display[['id', '日期', '商店名稱', '顯示品項', '金額', '分類結果']].copy()
        df_editor['日期'] = pd.to_datetime(df_editor['日期'])
        if len(date_range) == 2:
            start_d, end_d = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
            df_editor = df_editor[(df_editor['日期'] >= start_d) & (df_editor['日期'] <= end_d)]
        if search_term:
            df_editor = df_editor[df_editor['商店名稱'].astype(str).str.contains(search_term, case=False) | df_editor['顯示品項'].astype(str).str.contains(search_term, case=False) | df_editor['金額'].astype(str).str.contains(search_term)]
        
        df_editor = df_editor.sort_values('日期', ascending=False)
        df_editor = df_editor.rename(columns={'顯示品項': '品項'})
        df_editor['日期'] = df_editor['日期'].dt.strftime('%Y-%m-%d')
        df_editor.insert(0, "刪除", False)

        st.caption(f"共找到 {len(df_editor)} 筆資料")
        edited_df = st.data_editor(
            df_editor,
            column_config={
                "id": None,
                "刪除": st.column_config.CheckboxColumn(width="small"),
                "日期": st.column_config.TextColumn(disabled=True),
                "商店名稱": st.column_config.TextColumn(disabled=True),
                "品項": st.column_config.TextColumn(disabled=False),
                "金額": st.column_config.NumberColumn(disabled=False, min_value=0, format="$%d"), 
                "分類結果": st.column_config.SelectboxColumn("分類", options=CATEGORY_OPTIONS, required=True)
            },
            hide_index=True, use_container_width=True, key="detail_edit"
        )
        if st.button("💾 儲存明細變更 (含刪除)"):
            changes_count = 0
            deleted_count = 0
            for index, row in edited_df.iterrows():
                if row['刪除'] == True:
                    delete_transaction(row['id'])
                    deleted_count += 1
                    continue
                original_row = df_all[df_all['id'] == row['id']].iloc[0]
                if (row['分類結果'] != original_row['分類結果'] or row['品項'] != original_row['顯示品項'] or row['金額'] != original_row['金額']):
                    update_transaction(row['id'], row['品項'], row['金額'], row['分類結果'])
                    changes_count += 1
            if deleted_count > 0 or changes_count > 0: st.success(f"刪除 {deleted_count} 筆，更新 {changes_count} 筆！"); st.rerun()
            else: st.info("無變更")

    with tab3:
        if uk_count > 0:
            st.warning(f"👇 {selected_month} 有 {uk_count} 筆未分類！")
            unknown_df = df_display[df_display['分類結果']=='其他']
            suggestions = []
            store_stats = unknown_df[unknown_df['商店名稱'] != '-'].groupby('商店名稱')['金額'].agg(['sum', 'count']).reset_index()
            for _, row in store_stats.iterrows():
                suggestions.append({"關鍵字": row['商店名稱'], "類型": "商店", "參考金額": row['sum'], "筆數": row['count']})
            ignored_items = ['一般消費', '-', '', 'nan']
            item_stats = unknown_df[~unknown_df['品項'].isin(ignored_items)].groupby('品項')['金額'].agg(['sum', 'count']).reset_index()
            for _, row in item_stats.iterrows():
                if row['品項'] not in [s['關鍵字'] for s in suggestions]:
                    suggestions.append({"關鍵字": row['品項'], "類型": "品項", "參考金額": row['sum'], "筆數": row['count']})
            if suggestions:
                suggestion_df = pd.DataFrame(suggestions)
                suggestion_df['請選擇分類'] = None
                suggestion_df['預設品項(選填)'] = None
                edited_result = st.data_editor(
                    suggestion_df,
                    column_config={
                        "關鍵字": st.column_config.TextColumn(disabled=True),
                        "類型": st.column_config.TextColumn(disabled=True, width="small"),
                        "參考金額": st.column_config.NumberColumn(disabled=True, format="$%d"),
                        "筆數": st.column_config.NumberColumn(disabled=True, width="small"),
                        "請選擇分類": st.column_config.SelectboxColumn(options=CATEGORY_OPTIONS, required=True),
                        "預設品項(選填)": st.column_config.TextColumn()
                    },
                    hide_index=True, use_container_width=True, num_rows="fixed", key="quick_rule_v6"
                )
                if st.button("💾 儲存規則"):
                    for index, row in edited_result.iterrows():
                        if row['請選擇分類']: save_custom_rule(row['關鍵字'], row['請選擇分類'], row['預設品項(選填)'])
                    st.success("已更新！"); st.rerun()

        st.markdown("### ⚙️ 規則管理")
        st.caption("選取該列並按 Delete 可刪除規則。")
        rules_list = []
        for k, v in rules.items():
            rules_list.append({"刪除": False, "關鍵字": k, "分類": v['category'], "預設品項": v.get('item')})
        edited_rules = st.data_editor(
            pd.DataFrame(rules_list),
            column_config={
                "刪除": st.column_config.CheckboxColumn(width="small"),
                "關鍵字": st.column_config.TextColumn(required=True),
                "分類": st.column_config.SelectboxColumn(options=CATEGORY_OPTIONS, required=True),
                "預設品項": st.column_config.TextColumn()
            },
            num_rows="dynamic", use_container_width=True, hide_index=True, key="rules_editor"
        )
        if st.button("💾 儲存所有規則變更"):
            new_dict = {}
            for index, row in edited_rules.iterrows():
                if not row['刪除'] and row['關鍵字'] and row['分類']:
                    new_dict[row['關鍵字']] = {"category": row['分類'], "item": row['預設品項'] if row['預設品項'] else None}
            save_all_rules(new_dict)
            st.success("已更新！"); st.rerun()

    with tab4:
        st.subheader("✂️ 拆帳")
        recent_df = df_all.sort_values('日期', ascending=False).head(30)
        recent_df['label'] = recent_df.apply(lambda x: f"{x['id']} | {x['日期'].strftime('%Y-%m-%d')} | {x['商店名稱']} | ${x['金額']}", axis=1)
        selected_option = st.selectbox("選擇交易：", options=recent_df['label'])
        if selected_option:
            selected_id = int(selected_option.split(" | ")[0])
            target_row = df_all[df_all['id'] == selected_id].iloc[0]
            total_amount = target_row['金額']
            st.write(f"### 總金額：${total_amount}")
            if 'split_data' not in st.session_state or st.session_state.get('current_split_id') != selected_id:
                st.session_state.current_split_id = selected_id
                st.session_state.split_data = pd.DataFrame([{"品項": "", "金額": 0, "分類": "飲食"}, {"品項": "", "金額": 0, "分類": "日常用品"}])
            edited_split = st.data_editor(
                st.session_state.split_data,
                column_config={
                    "品項": st.column_config.TextColumn(required=True),
                    "金額": st.column_config.NumberColumn(required=True, min_value=0),
                    "分類": st.column_config.SelectboxColumn(options=CATEGORY_OPTIONS, required=True)
                },
                num_rows="dynamic", use_container_width=True, key="split_editor"
            )
            current_sum = edited_split['金額'].sum()
            remaining = total_amount - current_sum
            c1, c2 = st.columns(2)
            c1.metric("拆分總和", f"${current_sum}")
            c2.metric("剩餘", f"${remaining}", delta_color="normal" if remaining==0 else "inverse")
            if remaining == 0:
                if st.button("🚀 確認拆分"):
                    new_rows = []
                    for index, row in edited_split.iterrows():
                        if row['金額'] > 0:
                            new_rows.append({"date": target_row['日期'].strftime('%Y-%m-%d'), "store": target_row['商店名稱'], "item": row['品項'], "price": row['金額'], "fixed_category": row['分類']})
                    if split_transaction(selected_id, pd.DataFrame(new_rows)):
                        st.success("拆帳成功！"); del st.session_state['split_data']; st.rerun()
            else: st.warning("金額不符！")
else:
    st.info("👋 資料庫是空的，請開始使用！")