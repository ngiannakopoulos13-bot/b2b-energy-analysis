import streamlit as st
import pyodbc
import pandas as pd
import numpy as np
import re
from datetime import datetime
from sqlalchemy import create_engine

cost_engine = create_engine("sqlite:///cost_analysis.db")


def inject_custom_css():
    st.markdown("""
    <style>
    .stApp {
        background:
            radial-gradient(circle at top left, rgba(236, 72, 153, 0.22), transparent 28%),
            radial-gradient(circle at top right, rgba(168, 85, 247, 0.22), transparent 30%),
            radial-gradient(circle at bottom left, rgba(34, 197, 94, 0.16), transparent 28%),
            linear-gradient(160deg, #0B1020 0%, #111827 45%, #0F172A 100%);
    }
    .main { background: transparent; }
    .block-container {
        padding-top: 2.2rem;
        padding-bottom: 2.5rem;
        padding-left: 2.2rem;
        padding-right: 2.2rem;
        max-width: 1500px;
    }
    h1, h2, h3 {
        color: #F8FAFC;
        font-weight: 700;
        letter-spacing: -0.02em;
        margin-top: 0;
    }
    .section-title {
        font-size: 1.85rem;
        font-weight: 800;
        color: #F8FAFC;
        margin-top: 0.2rem;
        margin-bottom: 1rem;
        letter-spacing: -0.03em;
        line-height: 1.2;
        text-shadow: 0 0 18px rgba(168, 85, 247, 0.18);
    }
    .subsection-title {
        font-size: 1.15rem;
        font-weight: 700;
        color: #E2E8F0;
        margin-top: 1.15rem;
        margin-bottom: 0.7rem;
        line-height: 1.25;
    }
    .top-title-spacing { margin-bottom: 1.15rem; }
    .overview-spacing { margin-bottom: 0.55rem; }

    section[data-testid="stSidebar"] {
        background:
            linear-gradient(180deg, rgba(17, 24, 39, 0.96) 0%, rgba(15, 23, 42, 0.98) 100%);
        border-right: 1px solid rgba(168, 85, 247, 0.28);
        box-shadow: inset -1px 0 0 rgba(236, 72, 153, 0.10);
    }

    section[data-testid="stSidebar"] .stMarkdown,
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] div,
    section[data-testid="stSidebar"] p,
    section[data-testid="stSidebar"] span {
        color: #F8FAFC !important;
    }

    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 {
        color: #F9A8D4 !important;
        text-shadow: 0 0 12px rgba(236, 72, 153, 0.18);
    }

    div[data-baseweb="input"] > div,
    div[data-baseweb="select"] > div,
    div[data-testid="stDateInput"] > div > div,
    div[data-testid="stTextInput"] > div > div {
        background: rgba(15, 23, 42, 0.92) !important;
        border: 1px solid rgba(168, 85, 247, 0.35) !important;
        border-radius: 14px !important;
        color: #F8FAFC !important;
        transition: all 0.2s ease;
    }

    div[data-baseweb="input"] > div:focus-within,
    div[data-baseweb="select"] > div:focus-within,
    div[data-testid="stDateInput"] > div > div:focus-within,
    div[data-testid="stTextInput"] > div > div:focus-within {
        border: 1px solid rgba(34, 197, 94, 0.70) !important;
        box-shadow: 0 0 0 1px rgba(34, 197, 94, 0.18), 0 0 18px rgba(34, 197, 94, 0.12);
    }

    input, textarea { color: #F8FAFC !important; }

    .stRadio label, .stSelectbox label, .stDateInput label, .stTextInput label {
        color: #E2E8F0 !important;
        font-weight: 600 !important;
    }

    .stButton > button,
    .stDownloadButton > button,
    div[data-testid="stFormSubmitButton"] > button {
        background: linear-gradient(90deg, #EC4899 0%, #A855F7 50%, #22C55E 100%);
        color: white !important;
        border: none !important;
        border-radius: 14px !important;
        padding: 0.65rem 1.1rem !important;
        font-weight: 700 !important;
        box-shadow: 0 10px 24px rgba(0, 0, 0, 0.20);
    }

    div[data-testid="stMetric"] {
        background:
            linear-gradient(135deg, rgba(23, 32, 51, 0.92) 0%, rgba(30, 41, 59, 0.92) 100%);
        border: 1px solid rgba(168, 85, 247, 0.22);
        padding: 16px 18px 14px 18px;
        border-radius: 18px;
        box-shadow: 0 8px 24px rgba(0,0,0,0.22);
        min-height: 132px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        backdrop-filter: blur(6px);
    }

    div[data-testid="stMetricLabel"] {
        color: #86EFAC !important;
        font-weight: 700 !important;
        font-size: 0.95rem !important;
        padding-bottom: 0.15rem;
    }

    div[data-testid="stMetricValue"] {
        color: #F8FAFC !important;
        font-weight: 800 !important;
        font-size: 2.15rem !important;
        line-height: 1.05 !important;
    }

    div[data-testid="stDataFrame"] {
        border: 1px solid rgba(168, 85, 247, 0.25);
        border-radius: 14px;
        overflow: hidden;
        margin-top: 0.35rem;
        box-shadow: 0 8px 20px rgba(0,0,0,0.18);
    }

    .streamlit-expanderHeader {
        background: rgba(15, 23, 42, 0.65);
        border: 1px solid rgba(34, 197, 94, 0.18);
        border-radius: 12px;
        color: #F8FAFC !important;
    }

    div[data-testid="stAlert"] { border-radius: 14px; }
    </style>
    """, unsafe_allow_html=True)


def init_auth_state():
    if "db_connected" not in st.session_state:
        st.session_state.db_connected = False
    if "db_conn_params" not in st.session_state:
        st.session_state.db_conn_params = None


def try_connect(server, database, username, password):
    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=15;"
    )
    return pyodbc.connect(connection_string)


def render_login_box():
    st.sidebar.markdown("## Database Login")

    with st.sidebar.form("db_login_form"):
        server = st.text_input("Server", placeholder="my-server.database.windows.net")
        database = st.text_input("Database", placeholder="CDW")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Connect")

    if submitted:
        if not server or not database or not username or not password:
            st.sidebar.error("Συμπλήρωσε όλα τα πεδία.")
            return

        try:
            conn = try_connect(server, database, username, password)
            conn.close()

            st.session_state.db_connected = True
            st.session_state.db_conn_params = {
                "server": server,
                "database": database,
                "username": username,
                "password": password,
            }
            st.sidebar.success("Επιτυχής σύνδεση στη βάση.")
        except pyodbc.Error as e:
            st.session_state.db_connected = False
            st.session_state.db_conn_params = None
            st.sidebar.error(f"Database connection error: {e}")

    if st.session_state.db_connected:
        if st.sidebar.button("Disconnect"):
            st.session_state.db_connected = False
            st.session_state.db_conn_params = None
            st.rerun()


def get_connection():
    params = st.session_state.get("db_conn_params")

    if not params:
        st.error("Δεν υπάρχει ενεργή σύνδεση. Κάνε πρώτα login στη βάση.")
        st.stop()

    try:
        return try_connect(
            params["server"],
            params["database"],
            params["username"],
            params["password"],
        )
    except pyodbc.Error as e:
        st.error(f"Database connection error: {e}")
        st.stop()


def sanitize_filename_part(text):
    if text is None:
        return "EMPTY"

    text = str(text).strip()

    replacements = {
        "ΑΦΜ": "AFM",
        "ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ": "PAROXI",
        "ΟΜΙΛΟΣ": "OMILOS",
        "ΕΠΩΝΥΜΙΑ": "EPONYMIA"
    }

    if text in replacements:
        text = replacements[text]

    text = text.replace(" ", "_")
    text = re.sub(r"[^\w\-]", "_", text, flags=re.UNICODE)
    text = re.sub(r"_+", "_", text).strip("_")

    return text[:80] if text else "EMPTY"


def build_output_filename(filter_criteria, input_value):
    criteria_part = sanitize_filename_part(filter_criteria)
    value_part = sanitize_filename_part(input_value)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{criteria_part}_{value_part}_{timestamp}.xlsx"


def format_number_for_display(value):
    if pd.isna(value):
        return ""
    return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def transform_data(df, interval, unit="kWh"):
    data = []
    divisor = 1000 if unit == "MWh" else 1

    for _, row in df.iterrows():
        date = row['ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ']
        paroxis = row['ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ']
        tfm = row['ΤΦΜ']

        if interval == "Hourly":
            hour = 1
            for i in range(1, 97, 4):
                q_sum = 0
                for j in range(4):
                    val = pd.to_numeric(row.get(f'Q{i+j}', 0), errors='coerce')
                    q_sum += 0 if pd.isna(val) else val
                q_sum = q_sum / divisor
                data.append([date, paroxis, tfm, hour, q_sum])
                hour += 1
        else:
            for i in range(1, 97):
                q_value = pd.to_numeric(row.get(f'Q{i}', 0), errors='coerce')
                q_value = 0 if pd.isna(q_value) else q_value
                q_value = q_value / divisor
                data.append([date, paroxis, tfm, i, q_value])

    return pd.DataFrame(
        data,
        columns=['ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ', 'ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ', 'ΤΦΜ', 'Interval', 'CONSUMPTION']
    )


def label_arithmos_paroxis(df):
    df = df.copy()

    categories = {
        'DS': ['DS'],
        'CRETE': ['ΗΡΑΚΛΕΙΟ', 'ΛΑΣΙΘΙ', 'ΡΕΘΥΜΝΟ', 'ΧΑΝΙΑ'],
        'REST': [
            'ΑΜΟΡΓΟΣ', 'ΑΝΑΦΗ', 'ΔΩΔΕΚΑΝΗΣΟΥ', 'ΔΩΔΕΚΑΝΗΣΟΥ-ΡΟΔΟΣ',
            'ΚΥΘΝΟΣ', 'ΚΥΚΛΑΔΩΝ', 'ΛΕΣΒΟΣ', 'ΣΑΜΟΣ', 'ΣΕΡΙΦΟΣ', 'ΣΙΦΝΟΣ', 'ΧΙΟΣ'
        ]
    }

    df['ΤΥΠΟΣ ΤΑΣΗΣ'] = df['ΤΥΠΟΣ ΤΑΣΗΣ'].fillna('UNKNOWN')
    df['ΣΥΣΤΗΜΑ'] = df['ΣΥΣΤΗΜΑ'].fillna('UNKNOWN')

    conditions = [
        (df['ΤΥΠΟΣ ΤΑΣΗΣ'] == 'MV') & (df['ΣΥΣΤΗΜΑ'].isin(categories['DS'])),
        (df['ΤΥΠΟΣ ΤΑΣΗΣ'] == 'LV') & (df['ΣΥΣΤΗΜΑ'].isin(categories['DS'])),
        (df['ΤΥΠΟΣ ΤΑΣΗΣ'] == 'MV') & (df['ΣΥΣΤΗΜΑ'].isin(categories['CRETE'])),
        (df['ΤΥΠΟΣ ΤΑΣΗΣ'] == 'LV') & (df['ΣΥΣΤΗΜΑ'].isin(categories['CRETE'])),
        (df['ΤΥΠΟΣ ΤΑΣΗΣ'] == 'MV') & (df['ΣΥΣΤΗΜΑ'].isin(categories['REST'])),
        (df['ΤΥΠΟΣ ΤΑΣΗΣ'] == 'LV') & (df['ΣΥΣΤΗΜΑ'].isin(categories['REST'])),
    ]

    labels = ['MV DS', 'LV DS', 'MV CRETE', 'LV CRETE', 'MV REST', 'LV REST']
    df['Label'] = np.select(conditions, labels, default='UNKNOWN')
    return df


def generate_curves(transformed_df, original_df, include_all_curves=True):
    final_curves = {}
    total_label_curves = {}
    excluded_paroxis = []

    original_df = label_arithmos_paroxis(original_df.copy())
    paroxi_labels = original_df[['ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ', 'Label']].drop_duplicates()
    transformed_df = transformed_df.merge(paroxi_labels, on='ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ', how='left')

    for paroxis, group in transformed_df.groupby('ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ'):
        label = group['Label'].iloc[0] if not group['Label'].isnull().all() else 'UNKNOWN'

        active_plus = group[group['ΤΦΜ'].str.upper() == 'ACTIVE+']
        active_minus = group[group['ΤΦΜ'].str.upper() == 'ACTIVE-']

        active_plus_grouped = active_plus.groupby(
            ['ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ', 'Interval']
        )['CONSUMPTION'].sum().reset_index()

        active_minus_grouped = active_minus.groupby(
            ['ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ', 'Interval']
        )['CONSUMPTION'].sum().reset_index()

        merged = pd.merge(
            active_plus_grouped,
            active_minus_grouped,
            on=['ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ', 'Interval'],
            how='outer',
            suffixes=('_plus', '_minus')
        ).fillna(0)

        merged['FINAL_CONSUMPTION'] = merged['CONSUMPTION_plus'] - merged['CONSUMPTION_minus']

        p_minus_curve = group[group['ΤΦΜ'].astype(str).str.contains('P-', case=False, na=False)]
        if not p_minus_curve.empty:
            p_minus_grouped = p_minus_curve.groupby(
                ['ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ', 'Interval']
            )['CONSUMPTION'].sum().reset_index()

            merged = pd.merge(
                merged,
                p_minus_grouped,
                on=['ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ', 'Interval'],
                how='left'
            ).fillna(0)

            merged.rename(columns={'CONSUMPTION': 'P_MINUS_CURVE'}, inplace=True)

        merged['ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ'] = pd.to_datetime(
            merged['ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ'],
            errors='coerce'
        )

        monthly_sum = merged.groupby(
            merged['ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ'].dt.to_period('M')
        )['FINAL_CONSUMPTION'].sum()

        if (monthly_sum < 0).any():
            excluded_paroxis.append(paroxis)
            final_curves[paroxis] = merged
            continue

        if include_all_curves:
            final_curves[paroxis] = merged

        temp = merged[['ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ', 'Interval', 'FINAL_CONSUMPTION']].copy()
        temp['ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ'] = pd.to_datetime(
            temp['ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ'],
            errors='coerce'
        )

        if label not in total_label_curves:
            total_label_curves[label] = temp
        else:
            total_label_curves[label]['ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ'] = pd.to_datetime(
                total_label_curves[label]['ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ'],
                errors='coerce'
            )

            total_label_curves[label] = pd.merge(
                total_label_curves[label],
                temp,
                on=['ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ', 'Interval'],
                how='outer',
                suffixes=('', '_new')
            ).fillna(0)

            total_label_curves[label]['FINAL_CONSUMPTION'] += total_label_curves[label].pop('FINAL_CONSUMPTION_new')

    return final_curves, total_label_curves, excluded_paroxis


def calculate_supply_sums(curves_dict, df_labels):
    rows = []

    for paroxis, curve_df in curves_dict.items():
        total_sum = 0 if curve_df.empty else curve_df['FINAL_CONSUMPTION'].sum()

        label_arr = df_labels[df_labels['ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ'] == paroxis]['Label'].dropna().unique()
        label = label_arr[0] if len(label_arr) > 0 else "UNKNOWN"

        rows.append({
            'ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ': str(paroxis),
            'Label': label,
            'Total Sum': total_sum
        })

    result = pd.DataFrame(rows)

    if not result.empty:
        result = result.sort_values(['Label', 'ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ']).reset_index(drop=True)
        result['Total Sum Display'] = result['Total Sum'].apply(format_number_for_display)

    return result


def calculate_total_category_sums(total_label_curves):
    label_order = ['MV DS', 'LV DS', 'MV CRETE', 'LV CRETE', 'MV REST', 'LV REST']
    rows = []

    for label in label_order:
        if label in total_label_curves and not total_label_curves[label].empty:
            total_sum = total_label_curves[label]['FINAL_CONSUMPTION'].sum()
        else:
            total_sum = 0

        rows.append({
            'Category': label,
            'Total Sum': total_sum,
            'Total Sum Display': format_number_for_display(total_sum)
        })

    return pd.DataFrame(rows)


def calculate_overall_total(total_label_curves):
    total = 0
    for _, curve_df in total_label_curves.items():
        if not curve_df.empty:
            total += curve_df['FINAL_CONSUMPTION'].sum()
    return total


def map_8_to_11_figures(paroxis_list, conn):
    mapped_paroxis = []
    unmapped = []

    for paroxis in paroxis_list:
        query = """
            SELECT DISTINCT [ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ]
            FROM EPGT.B2B_CUR_MEASUREMENTS
            WHERE [ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ] LIKE ?
        """
        try:
            result = pd.read_sql(query, conn, params=[f"%{paroxis}%"])
            if len(result) == 1:
                mapped_paroxis.append(result.iloc[0]['ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ'])
            elif len(result) > 1:
                st.warning(f"Multiple matches found for partial value {paroxis}. Please refine your input.")
            else:
                st.warning(f"No matching ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ found for {paroxis}.")
                unmapped.append(paroxis)
        except Exception as e:
            st.error(f"Error mapping ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ {paroxis}: {e}")

    if unmapped:
        st.warning(f"Unmapped partial ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ values: {', '.join(unmapped)}")

    return mapped_paroxis


def get_omilos_or_eponymia(input_value, conn):
    query = """
        SELECT DISTINCT [ΟΜΙΛΟΣ], [ΕΠΩΝΥΜΙΑ]
        FROM EPGT.B2B_CUR_MEASUREMENTS
        WHERE [ΑΦΜ] = ? OR [ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ] = ?
    """
    try:
        result = pd.read_sql(query, conn, params=[input_value, input_value])
        if not result.empty:
            omilos = result['ΟΜΙΛΟΣ'].dropna().iloc[0] if 'ΟΜΙΛΟΣ' in result and not result['ΟΜΙΛΟΣ'].isnull().all() else None
            eponymia = result['ΕΠΩΝΥΜΙΑ'].dropna().iloc[0] if 'ΕΠΩΝΥΜΙΑ' in result and not result['ΕΠΩΝΥΜΙΑ'].isnull().all() else None
            return omilos, eponymia
    except Exception as e:
        st.error(f"Error retrieving ΟΜΙΛΟΣ or ΕΠΩΝΥΜΙΑ: {e}")
    return None, None


def render_dashboard_summary(df, excluded=None, overall_total_display="0,00", unit="kWh"):
    st.markdown('<div class="overview-spacing section-title">⚡ Energy Portfolio Overview</div>', unsafe_allow_html=True)

    if df.empty:
        st.info("Δεν υπάρχουν δεδομένα για dashboard.")
        return

    excluded = excluded if excluded is not None else []

    labeled_df = label_arithmos_paroxis(df.copy())
    unique_supplies = labeled_df[['ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ', 'Label']].drop_duplicates()

    label_order = ['MV DS', 'LV DS', 'MV CRETE', 'LV CRETE', 'MV REST', 'LV REST']

    counts = (
        unique_supplies.groupby('Label')['ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ']
        .nunique()
        .reindex(label_order, fill_value=0)
    )

    total_supplies = unique_supplies['ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ'].nunique()
    total_afm = labeled_df['ΑΦΜ'].nunique() if 'ΑΦΜ' in labeled_df.columns else 0

    p_minus_supplies = labeled_df[
        labeled_df['ΤΦΜ'].astype(str).str.contains('P-', case=False, na=False)
    ][['ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ']].drop_duplicates()['ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ'].nunique()

    row1_col1, row1_col2, row1_col3, row1_col4, row1_col5 = st.columns(5)
    with row1_col1:
        st.metric("🔌 Total Supplies", total_supplies)
    with row1_col2:
        st.metric("🧾 Total VAT IDs", total_afm)
    with row1_col3:
        st.metric("➖ Supplies with P-", p_minus_supplies)
    with row1_col4:
        st.metric("⚠️ Excluded Supplies", len(excluded))
    with row1_col5:
        st.metric(f"💡 Total Consumption ({unit})", overall_total_display)

    st.markdown('<div class="subsection-title">📍 Supply Mix by Network Category</div>', unsafe_allow_html=True)

    row2_col1, row2_col2, row2_col3 = st.columns(3)
    row3_col1, row3_col2, row3_col3 = st.columns(3)

    with row2_col1:
        st.metric("⚡ MV DS", int(counts['MV DS']))
    with row2_col2:
        st.metric("🔌 LV DS", int(counts['LV DS']))
    with row2_col3:
        st.metric("🏝️ MV CRETE", int(counts['MV CRETE']))

    with row3_col1:
        st.metric("🏖️ LV CRETE", int(counts['LV CRETE']))
    with row3_col2:
        st.metric("🌍 MV REST", int(counts['MV REST']))
    with row3_col3:
        st.metric("🌐 LV REST", int(counts['LV REST']))


def main():
    st.set_page_config(page_title="B2B Data Analysis Tool", layout="wide")
    inject_custom_css()
    init_auth_state()

    st.markdown('<div class="top-title-spacing section-title">⚡ B2B Energy Analytics Hub</div>', unsafe_allow_html=True)

    render_login_box()

    if not st.session_state.db_connected:
        st.info("Συμπλήρωσε τα database credentials στο sidebar και πάτα Connect.")
        st.stop()

    st.sidebar.header("Input Parameters")

    filter_criteria = st.sidebar.selectbox(
        "Select Filtering Criteria:",
        ["ΟΜΙΛΟΣ", "ΕΠΩΝΥΜΙΑ", "ΑΦΜ", "ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ"],
        key="filter_criteria"
    )

    input_value = st.sidebar.text_input(
        f"Enter {filter_criteria} value:",
        key="input_value"
    )

    start_date = st.sidebar.date_input("Start Date", datetime.today(), key="start_date")
    end_date = st.sidebar.date_input("End Date", datetime.today(), key="end_date")

    interval = st.sidebar.selectbox(
        "Choose Data Interval:",
        ["15-Minute", "Hourly"],
        key="data_interval"
    )

    unit = st.sidebar.selectbox(
        "Choose Unit:",
        ["kWh", "MWh"],
        key="unit_selector"
    )

    include_all_curves = st.sidebar.radio(
        "Θέλεις να δημιουργηθούν curves για όλες τις παροχές;",
        ["Ναι", "Όχι"],
        index=0,
        key="include_all_curves_radio"
    ) == "Ναι"

    conn = get_connection()
    omilos, eponymia = get_omilos_or_eponymia(input_value, conn)

    if filter_criteria == "ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ":
        filter_value = map_8_to_11_figures([input_value], conn)
        if not filter_value:
            st.warning("No valid 11-figure ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ values found. Stopping analysis.")
            conn.close()
            return
    else:
        filter_value = [input_value]

    related_choice = None
    if omilos or eponymia:
        group_label = omilos if omilos else eponymia
        related_choice = st.sidebar.radio(
            f"Do you want data for all ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ of '{group_label}' or just for the input {filter_criteria}?",
            ["All ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ", f"Just this {filter_criteria}"],
            key="related_choice"
        )

    if st.sidebar.button("Run Analysis", key="run_analysis"):
        if related_choice == "All ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ":
            query = """
                SELECT DISTINCT [ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ]
                FROM EPGT.B2B_CUR_MEASUREMENTS
                WHERE [ΟΜΙΛΟΣ] = ? OR [ΕΠΩΝΥΜΙΑ] = ?
            """
            related_paroxis = pd.read_sql(query, conn, params=[omilos, eponymia])
            filter_value = related_paroxis['ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ'].tolist()
            st.info(f"Found {len(filter_value)} related ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ for '{group_label}'.")

            if not filter_value:
                st.warning("Δεν βρέθηκαν σχετικές παροχές.")
                conn.close()
                return

            query = f"""
                SELECT *
                FROM EPGT.B2B_CUR_MEASUREMENTS
                WHERE [ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ] IN ({', '.join(['?' for _ in filter_value])})
                  AND [ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ] BETWEEN ? AND ?
            """
            df = pd.read_sql(query, conn, params=filter_value + [start_date, end_date])

        else:
            query = f"""
                SELECT *
                FROM EPGT.B2B_CUR_MEASUREMENTS
                WHERE [{filter_criteria}] IN ({', '.join(['?' for _ in filter_value])})
                  AND [ΗΜΕΡΟΜΗΝΙΑ ΜΕΤΡΗΣΗΣ] BETWEEN ? AND ?
            """
            df = pd.read_sql(query, conn, params=filter_value + [start_date, end_date])

        conn.close()

        if df.empty:
            st.warning("No data found for the specified criteria.")
            return

        df = label_arithmos_paroxis(df)
        transformed_df = transform_data(df, interval, unit=unit)

        curves, total_label_curves, excluded = generate_curves(
            transformed_df,
            df,
            include_all_curves=include_all_curves
        )

        supply_sums_df = calculate_supply_sums(curves, df)
        total_category_sums_df = calculate_total_category_sums(total_label_curves)
        overall_total = calculate_overall_total(total_label_curves)
        overall_total_display = format_number_for_display(overall_total)

        render_dashboard_summary(
            df,
            excluded=excluded,
            overall_total_display=overall_total_display,
            unit=unit
        )

        st.markdown("---")
        st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
        st.markdown('<div class="section-title">📊 Consumption by Network Category</div>', unsafe_allow_html=True)

        metric_cols_top = st.columns(3)
        metric_cols_bottom = st.columns(3)

        category_display_map = {
            'MV DS': 'Total MV DS',
            'LV DS': 'Total LV DS',
            'MV CRETE': 'Total MV CRETE',
            'LV CRETE': 'Total LV CRETE',
            'MV REST': 'Total MV REST',
            'LV REST': 'Total LV REST'
        }

        top_labels = ['MV DS', 'LV DS', 'MV CRETE']
        bottom_labels = ['LV CRETE', 'MV REST', 'LV REST']

        total_sums_lookup = {
            row['Category']: row['Total Sum Display']
            for _, row in total_category_sums_df.iterrows()
        }

        for col, label in zip(metric_cols_top, top_labels):
            with col:
                st.metric(category_display_map[label], total_sums_lookup.get(label, "0,00"))

        for col, label in zip(metric_cols_bottom, bottom_labels):
            with col:
                st.metric(category_display_map[label], total_sums_lookup.get(label, "0,00"))

        st.markdown("<div style='height: 6px;'></div>", unsafe_allow_html=True)
        st.markdown('<div class="section-title">🧮 Consumption per Supply Point</div>', unsafe_allow_html=True)

        if supply_sums_df.empty:
            st.info("Δεν υπάρχουν διαθέσιμα sums ανά παροχή.")
        else:
            label_order = ['MV DS', 'LV DS', 'MV CRETE', 'LV CRETE', 'MV REST', 'LV REST']

            for label in label_order:
                category_df = supply_sums_df[supply_sums_df['Label'] == label].copy()
                if category_df.empty:
                    continue

                category_total = category_df['Total Sum'].sum()
                category_total_display = format_number_for_display(category_total)

                with st.expander(f"{label} • {category_total_display} {unit}", expanded=(label == "MV DS")):
                    display_supply_sums = category_df[['ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ', 'Label', 'Total Sum Display']].rename(
                        columns={'Total Sum Display': f'Total Sum ({unit})'}
                    )
                    st.dataframe(display_supply_sums, use_container_width=True, hide_index=True)

        curve_file = build_output_filename(filter_criteria, input_value)

        with pd.ExcelWriter(curve_file, engine="xlsxwriter") as writer:
            if include_all_curves:
                for paroxis, curve_df in curves.items():
                    label = df[df['ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ'] == paroxis]['Label'].dropna().unique()
                    label = label[0] if len(label) > 0 else "UNKNOWN"
                    sheet_name = f"Paroxis_{paroxis}_{label}"[:31]
                    curve_df.to_excel(writer, sheet_name=sheet_name, index=False)

            for paroxis in excluded:
                df_ex = curves[paroxis]
                label = df[df['ΑΡΙΘΜΟΣ ΠΑΡΟΧΗΣ'] == paroxis]['Label'].dropna().unique()
                label = label[0] if len(label) > 0 else "UNKNOWN"
                sheet_name = f"Excluded_{paroxis}_{label}"[:31]
                df_ex.to_excel(writer, sheet_name=sheet_name, index=False)

            for label, curve_df in total_label_curves.items():
                sheet_name = f"Total_{label}"[:31]
                curve_df.to_excel(writer, sheet_name=sheet_name, index=False)

        st.success(f"Curve generation complete. File created: {curve_file}")

        with open(curve_file, "rb") as f:
            st.download_button(
                "Download Curves File",
                data=f,
                file_name=curve_file
            )

        if excluded:
            st.warning(
                f"Excluded παροχές λόγω αρνητικού monthly sum: {', '.join(map(str, excluded))}"
            )


if __name__ == "__main__":
    main()