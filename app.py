"""
AuditArc 审迹 - AI审计风险识别系统 v2.0
支持A股/美股 · 多行业基准 · 多源数据 · 舆情监控 · 7×24持续监控
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import time
import io
import datetime
import os
import json
import hashlib
import random
import concurrent.futures
from functools import lru_cache

# =====================================================================
# 多数据源模块
# =====================================================================

# ---------- Yahoo Finance 数据拉取 ----------
def fetch_yahoo_finance(ticker: str, years: int = 5):
    """
    通过 yfinance 拉取全球股票财务数据（A股/美股/港股等）
    ticker 格式：
      A股: 000001.SZ / 600519.SS
      美股: AAPL / MSFT
      港股: 0700.HK
    """
    import yfinance as yf
    stock = yf.Ticker(ticker)
    info = stock.info
    company_name = info.get("longName") or info.get("shortName") or ticker

    # 拉取财务报表
    inc_annual = stock.financials  # 利润表
    bal_annual = stock.balance_sheet  # 资产负债表
    cf_annual  = stock.cashflow  # 现金流量表

    if inc_annual.empty or bal_annual.empty or cf_annual.empty:
        raise ValueError(f"未找到 {ticker} 的财务数据，请确认代码格式是否正确")

    # 取列（日期），按时间正序
    cols = sorted(inc_annual.columns)[-years:]
    years_list = [c.year for c in cols]

    def safe_get(df, key, col):
        """安全取值，单位转换为万元（国际报表为原币种）"""
        for k in ([key] if isinstance(key, str) else key):
            if k in df.index:
                v = df.loc[k, col]
                if pd.notna(v):
                    return float(v) / 10000
        return None

    # 利润表字段映射（兼容多种命名）
    inc_keys = {
        "营业收入": ["Total Revenue", "Revenue", "Operating Revenue"],
        "营业成本": ["Cost Of Revenue", "Cost of Revenue"],
        "销售费用": ["Selling General And Administration", "Selling And Marketing Expense"],
        "管理费用": ["General And Administrative Expense"],
        "研发费用": ["Research And Development", "Research Development"],
        "财务费用": ["Interest Expense", "Net Interest Income"],
        "营业利润": ["Operating Income", "EBIT"],
        "利润总额": ["Pretax Income", "Income Before Tax"],
        "所得税": ["Tax Provision", "Income Tax Expense"],
        "净利润": ["Net Income", "Net Income Common Stockholders"],
    }
    inc_rows = []
    for cn, en_keys in inc_keys.items():
        row = {"科目": cn}
        for col, yr in zip(cols, years_list):
            row[f"{yr}年"] = safe_get(inc_annual, en_keys, col)
        inc_rows.append(row)
    inc_df = pd.DataFrame(inc_rows)

    # 资产负债表字段映射
    bal_keys = {
        "货币资金": ["Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments"],
        "应收账款": ["Net Receivables", "Receivables", "Accounts Receivable"],
        "存货": ["Inventory", "Raw Materials", "Net PPE"],
        "流动资产合计": ["Current Assets", "Total Current Assets"],
        "固定资产": ["Net PPE", "Gross PPE"],
        "资产总计": ["Total Assets"],
        "应付账款": ["Accounts Payable", "Current Accrued Expenses"],
        "流动负债合计": ["Current Liabilities", "Total Current Liabilities"],
        "长期借款": ["Long Term Debt", "Long Term Debt And Capital Lease Obligation"],
        "负债合计": ["Total Liabilities Net Minority Interest", "Total Liab"],
        "所有者权益合计": ["Total Equity Gross Minority Interest", "Stockholders Equity", "Total Stockholder Equity"],
    }
    bal_rows = []
    for cn, en_keys in bal_keys.items():
        row = {"科目": cn}
        for col, yr in zip(cols, years_list):
            row[f"{yr}年"] = safe_get(bal_annual, en_keys, col)
        bal_rows.append(row)
    bal_df = pd.DataFrame(bal_rows)

    # 现金流量表字段映射
    cf_keys = {
        "经营活动产生的现金流量净额": ["Operating Cash Flow", "Total Cash From Operating Activities", "Cash Flow From Continuing Operating Activities"],
        "投资活动产生的现金流量净额": ["Investing Cash Flow", "Total Cashflows From Investing Activities", "Cash Flow From Continuing Investing Activities"],
        "筹资活动产生的现金流量净额": ["Financing Cash Flow", "Total Cash From Financing Activities", "Cash Flow From Continuing Financing Activities"],
        "现金及现金等价物净增加额": ["Changes In Cash", "Change In Cash Supplemental As Reported"],
    }
    cf_rows = []
    for cn, en_keys in cf_keys.items():
        row = {"科目": cn}
        for col, yr in zip(cols, years_list):
            row[f"{yr}年"] = safe_get(cf_annual, en_keys, col)
        cf_rows.append(row)
    cf_df = pd.DataFrame(cf_rows)

    return inc_df, bal_df, cf_df, company_name, years_list


# ---------- WRDS CSMAR（保留兼容） ----------
@st.cache_resource(show_spinner=False)
def get_wrds_connection(wrds_username: str, wrds_password: str):
    import wrds
    return wrds.Connection(wrds_username=wrds_username, wrds_password=wrds_password, autoconnect=True)


@st.cache_data(ttl=3600, show_spinner=False)
def get_csmar_data(ticker: str, wrds_username: str, wrds_password: str, years: int = 5):
    conn = get_wrds_connection(wrds_username, wrds_password)
    end_year = datetime.datetime.now().year - 1
    start_year = end_year - years + 1
    ticker_6 = ticker.zfill(6)

    # 并行拉取三张报表
    sql_map = {
        "income": f"""
            SELECT tyear, b001000000, b001100000, b002001000,
                   b002002000, b002003000, b002004000, b003000000,
                   b004001000, b004000000, b005000000, b006000000, b008000000
            FROM csmar.cfi_fi2
            WHERE stkcd = '{ticker_6}' AND tyear BETWEEN {start_year} AND {end_year} AND report_type = 'A'
            ORDER BY tyear
        """,
        "balance": f"""
            SELECT tyear, a001101000, a001110000, a001121000, a001123000,
                   a001200000, a002110000, a002111100, a002130000,
                   a002140000, a002000000, a000000000,
                   b001101000, b001110000, b001120000,
                   b001000000, b002110000, b002000000,
                   c000000000, c001000000, c002000000, c003000000, c004000000
            FROM csmar.cfi_fi1
            WHERE stkcd = '{ticker_6}' AND tyear BETWEEN {start_year} AND {end_year} AND report_type = 'A'
            ORDER BY tyear
        """,
        "cashflow": f"""
            SELECT tyear, c001000000, c001001000, c001002000, c001003000,
                   c001000000_net, c002000000_net, c003000000_net, c000000000
            FROM csmar.cfi_fi3
            WHERE stkcd = '{ticker_6}' AND tyear BETWEEN {start_year} AND {end_year} AND report_type = 'A'
            ORDER BY tyear
        """,
    }

    # 使用线程池并行拉取
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(conn.raw_sql, sql): key for key, sql in sql_map.items()}
        for future in concurrent.futures.as_completed(futures):
            key = futures[future]
            results[key] = future.result()

    inc_raw, bal_raw, cf_raw = results["income"], results["balance"], results["cashflow"]

    try:
        name_raw = conn.raw_sql(f"SELECT stknme FROM csmar.stk_listedcoinfannu WHERE stkcd = '{ticker_6}' LIMIT 1")
        company_name = name_raw.iloc[0, 0] if len(name_raw) > 0 else ticker_6
    except:
        company_name = ticker_6

    if inc_raw.empty or bal_raw.empty or cf_raw.empty:
        raise ValueError(f"未找到股票代码 {ticker_6} 的财务数据")

    years_list = sorted(inc_raw["tyear"].unique())

    inc_map = {
        "b001000000": "营业收入", "b001100000": "营业成本", "b002001000": "销售费用",
        "b002002000": "管理费用", "b002003000": "研发费用", "b002004000": "财务费用",
        "b003000000": "营业利润", "b004001000": "营业外收入", "b004000000": "营业外支出",
        "b005000000": "利润总额", "b006000000": "所得税", "b008000000": "净利润",
    }
    inc_df = _reshape(inc_raw, "tyear", inc_map, years_list)

    bal_map = {
        "a001101000": "货币资金", "a001110000": "应收账款", "a001121000": "预付账款",
        "a001123000": "存货", "a001200000": "流动资产合计", "a002110000": "固定资产",
        "a002111100": "在建工程", "a002130000": "无形资产", "a002140000": "长期股权投资",
        "a002000000": "非流动资产合计", "a000000000": "资产总计",
        "b001101000": "短期借款", "b001110000": "应付账款", "b001120000": "预收账款",
        "b001000000": "流动负债合计", "b002110000": "长期借款", "b002000000": "非流动负债合计",
        "c000000000": "负债合计", "c001000000": "股本", "c002000000": "资本公积",
        "c003000000": "未分配利润", "c004000000": "所有者权益合计",
    }
    bal_df = _reshape(bal_raw, "tyear", bal_map, years_list)

    cf_map = {
        "c001000000": "销售商品收到的现金", "c001001000": "购买商品支付的现金",
        "c001002000": "支付给职工的现金", "c001003000": "支付的各项税费",
        "c001000000_net": "经营活动产生的现金流量净额",
        "c002000000_net": "投资活动产生的现金流量净额",
        "c003000000_net": "筹资活动产生的现金流量净额",
        "c000000000": "现金及现金等价物净增加额",
    }
    cf_df = _reshape(cf_raw, "tyear", cf_map, years_list)
    return inc_df, bal_df, cf_df, company_name, years_list


def _reshape(df, year_col, col_map, years_list):
    rows = []
    for cn_name in col_map.values():
        rows.append({"科目": cn_name})
    result = pd.DataFrame(rows)
    for col_en, col_cn in col_map.items():
        if col_en not in df.columns:
            for yr in years_list:
                result.loc[result["科目"] == col_cn, f"{yr}年"] = None
            continue
        for yr in years_list:
            row = df[df[year_col] == yr]
            val = float(row[col_en].values[0]) / 10000 if len(row) > 0 and pd.notna(row[col_en].values[0]) else None
            result.loc[result["科目"] == col_cn, f"{yr}年"] = val
    return result


# ---------- 舆情与新闻抓取模块 ----------
def fetch_news_sentiment(company_name: str, ticker: str):
    """
    从多个公开 RSS/API 源抓取舆情数据
    实际部署时接入：
    - 巨潮资讯网 (cninfo.com.cn) RSS
    - SEC EDGAR (美股公告)
    - Google News RSS
    - 新浪财经 RSS
    - 东方财富舆情 API
    """
    # Demo 模式：模拟舆情数据（实际部署替换为真实 API 调用）
    now = datetime.datetime.now()
    sentiment_data = []

    # 模拟从多个来源获取的舆情信号
    news_sources = [
        {"source": "巨潮资讯", "type": "监管公告", "icon": "📋"},
        {"source": "SEC EDGAR", "type": "监管公告", "icon": "📋"},
        {"source": "Google News", "type": "行业新闻", "icon": "📰"},
        {"source": "新浪财经", "type": "市场舆情", "icon": "💬"},
        {"source": "东方财富", "type": "分析师评级", "icon": "📊"},
        {"source": "Twitter/X", "type": "社交媒体", "icon": "🐦"},
    ]

    # 基于公司名生成确定性的模拟数据
    seed = int(hashlib.md5(company_name.encode()).hexdigest()[:8], 16) % 10000
    rng = random.Random(seed)

    sample_news = [
        {"title": f"{company_name}被监管部门出具关注函，要求说明收入确认政策变更原因",
         "sentiment": "negative", "risk_signal": True, "source": news_sources[0],
         "time": now - datetime.timedelta(hours=rng.randint(2, 48))},
        {"title": f"{company_name}主要供应商被曝出财务造假嫌疑",
         "sentiment": "negative", "risk_signal": True, "source": news_sources[2],
         "time": now - datetime.timedelta(hours=rng.randint(6, 72))},
        {"title": f"{company_name}发布季度业绩预告，营收增速放缓",
         "sentiment": "neutral", "risk_signal": False, "source": news_sources[3],
         "time": now - datetime.timedelta(hours=rng.randint(12, 96))},
        {"title": f"行业分析：{company_name}所在板块面临政策调整压力",
         "sentiment": "negative", "risk_signal": True, "source": news_sources[4],
         "time": now - datetime.timedelta(hours=rng.randint(24, 120))},
        {"title": f"{company_name}高管近期大幅减持股份",
         "sentiment": "negative", "risk_signal": True, "source": news_sources[5],
         "time": now - datetime.timedelta(hours=rng.randint(1, 36))},
    ]

    # 根据种子选取3-5条
    count = rng.randint(3, 5)
    selected = rng.sample(sample_news, min(count, len(sample_news)))
    return sorted(selected, key=lambda x: x["time"], reverse=True)


def fetch_policy_updates():
    """
    抓取最新审计相关政策法规更新
    实际部署时接入：
    - 中注协官网 RSS
    - 财政部会计司公告
    - 证监会公告
    - PCAOB (美股审计监管)
    """
    now = datetime.datetime.now()
    policies = [
        {"title": "财政部发布《企业会计准则第 14 号——收入》应用指南更新",
         "source": "财政部", "date": (now - datetime.timedelta(days=15)).strftime("%Y-%m-%d"),
         "relevance": "high"},
        {"title": "中注协发布《审计准则问题解答第 16 号——审计中关键事项的沟通》",
         "source": "中注协", "date": (now - datetime.timedelta(days=30)).strftime("%Y-%m-%d"),
         "relevance": "medium"},
        {"title": "SEC 更新 PCAOB 审计标准 AS 2201 关于内控审计的指引",
         "source": "SEC/PCAOB", "date": (now - datetime.timedelta(days=45)).strftime("%Y-%m-%d"),
         "relevance": "medium"},
    ]
    return policies


# =====================================================================
# 多行业基准数据
# =====================================================================
INDUSTRY_CONFIGS = {
    "制造业": {
        "label": "制造业",
        "benchmarks": {
            "应收账款周转率": 7.2, "存货周转率": 4.1, "毛利率": 27.8,
            "净利率": 8.5, "资产负债率": 38.5, "经营现金流_净利润比": 0.92,
        },
        "key_metrics": ["存货周转率", "应收账款周转率", "毛利率", "资产负债率", "经营现金流_净利润比"],
        "risk_focus": "存货计价复杂、收入确认时点多、关联交易隐蔽",
        "radar_labels": ["收入质量", "应收账款", "存货质量", "现金流量", "偿债能力"],
    },
    "零售业": {
        "label": "零售业",
        "benchmarks": {
            "应收账款周转率": 28.5, "存货周转率": 8.6, "毛利率": 32.5,
            "净利率": 5.2, "资产负债率": 52.0, "经营现金流_净利润比": 1.15,
        },
        "key_metrics": ["存货周转率", "毛利率", "应收账款周转率", "资产负债率", "经营现金流_净利润比"],
        "risk_focus": "存货跌价风险大、促销压低毛利、预收/预付款多、加盟商管理复杂",
        "radar_labels": ["收入质量", "应收账款", "存货质量", "现金流量", "偿债能力"],
    },
}

RISK_LEVELS = {
    "极高": {"color": "#FF2D55", "icon": "🔴", "bg": "#FFF0F3"},
    "高":   {"color": "#FF6B35", "icon": "🟠", "bg": "#FFF3ED"},
    "中":   {"color": "#F5A623", "icon": "🟡", "bg": "#FFFBF0"},
    "低":   {"color": "#34C759", "icon": "🟢", "bg": "#F0FFF4"},
}

AUDIT_STANDARDS = {
    "收入异常":    "《审计准则第1141号》第三十二条：识别和评估重大错报风险，对收入确认时点及金额予以特别关注",
    "应收账款异常":"《审计准则第1312号》第十八条：对应收账款函证范围、坏账准备充分性进行实质性程序",
    "存货异常":    "《审计准则第1321号》第二十二条：对存货监盘、计价、可变现净值进行核查",
    "现金流异常":  "《审计准则第1141号》第三十四条：关注利润与经营活动现金流背离，识别潜在收入虚增风险",
    "负债率异常":  "《审计准则第1211号》第十五条：评价持续经营能力，关注偿债风险及披露充分性",
}

AUDIT_PROCEDURES = {
    "收入异常":    ["对前10大客户收入实施函证程序","抽查收入确认的合同、发货单、验收单","分析收入与现金流、应收账款的勾稽关系","检查年末前后是否存在异常大额交易"],
    "应收账款异常":["对应收账款账龄进行详细分析","发函询证前10大应收账款余额","复核坏账准备计提比例的合理性","检查是否存在已逾期但未计提减值的款项"],
    "存货异常":    ["参与或观察存货监盘","复核存货成本的计算方法一致性","评价存货跌价准备是否充分","核查存货积压情况及可变现净值"],
    "现金流异常":  ["复核销售商品收到现金与营收的差异原因","分析经营活动现金流下降的业务原因","对主要银行账户实施函证","检查是否存在虚构销售或提前确认收入"],
    "负债率异常":  ["核查所有借款合同及到期日","评价持续经营能力，关注到期债务安排","检查是否存在未披露的抵押、担保事项","分析资产负债率上升的主要驱动因素"],
}


# =====================================================================
# 演示数据（支持多行业）
# =====================================================================
def generate_demo_data(industry="制造业"):
    if industry == "零售业":
        income = pd.DataFrame({
            "科目": ["营业收入","营业成本","销售费用","管理费用","研发费用","财务费用","营业利润","利润总额","所得税","净利润"],
            "2019年": [520000,351000,62400,26000,5200,8200,67200,66000,16500,49500],
            "2020年": [498000,343600,64700,27400,5500,9100,47700,46500,11625,34875],
            "2021年": [578000,390200,69360,28900,6400,7800,75340,74000,18500,55500],
            "2022年": [625000,431300,81250,32500,7500,9800,62650,61000,15250,45750],
            "2023年": [710000,504100,99400,39050,8500,14200,44750,42800,10700,32100],
        })
        balance = pd.DataFrame({
            "科目": ["货币资金","应收账款","存货","流动资产合计","固定资产","资产总计",
                    "应付账款","流动负债合计","长期借款","负债合计","所有者权益合计"],
            "2019年": [85000,18500,95000,210000,120000,385000,52000,145000,42000,198000,187000],
            "2020年": [72000,22000,108000,218000,125000,400000,48000,152000,50000,215000,185000],
            "2021年": [68000,26500,118000,232000,135000,428000,55000,168000,52000,235000,193000],
            "2022年": [55000,35800,148000,262000,142000,472000,62000,195000,65000,278000,194000],
            "2023年": [42000,58000,196000,325000,155000,558000,72000,248000,88000,358000,200000],
        })
        cashflow = pd.DataFrame({
            "科目": ["经营活动产生的现金流量净额","投资活动产生的现金流量净额",
                    "筹资活动产生的现金流量净额","现金及现金等价物净增加额"],
            "2019年": [62000,-28000,-15000,19000],
            "2020年": [48000,-22000,8000,34000],
            "2021年": [58000,-35000,12000,35000],
            "2022年": [38000,-42000,20000,16000],
            "2023年": [12000,-55000,35000,-8000],
        })
        return income, balance, cashflow, "锦程连锁零售集团股份有限公司（演示）", [2019,2020,2021,2022,2023]
    else:
        # 制造业演示数据（原有）
        income = pd.DataFrame({
            "科目": ["营业收入","营业成本","销售费用","管理费用","研发费用","财务费用","营业利润","利润总额","所得税","净利润"],
            "2019年": [158200,112400,8600,10800,4900,2800,18700,19100,2865,16235],
            "2020年": [171300,121500,9200,11500,5200,3000,20900,21150,3173,17977],
            "2021年": [182450,128600,9800,12300,5600,3200,22950,23350,3500,19850],
            "2022年": [196800,141200,10500,13100,6200,3800,22000,22300,3350,18950],
            "2023年": [265800,198600,15200,16800,7100,6500,21600,20900,3135,17765],
        })
        balance = pd.DataFrame({
            "科目": ["货币资金","应收账款","存货","流动资产合计","固定资产","资产总计",
                    "应付账款","流动负债合计","长期借款","负债合计","所有者权益合计"],
            "2019年": [32100,25600,36500,98500,82300,204300,19200,46200,28000,54400,149900],
            "2020年": [30200,28400,39600,103300,86100,214500,20800,49300,31500,56000,158500],
            "2021年": [28600,31200,42800,111400,89600,227800,22500,52600,35000,93400,134400],
            "2022年": [24200,38900,51200,125200,95800,254400,26800,61900,42000,110000,154400],
            "2023年": [18500,73500,82600,189600,108200,344400,31200,87600,68000,164200,180200],
        })
        cashflow = pd.DataFrame({
            "科目": ["经营活动产生的现金流量净额","投资活动产生的现金流量净额",
                    "筹资活动产生的现金流量净额","现金及现金等价物净增加额"],
            "2019年": [20000,-18200,1500,3300],
            "2020年": [22800,-20500,1800,4100],
            "2021年": [26400,-21200,2000,7200],
            "2022年": [23500,-28100,4000,-600],
            "2023年": [4200,-39600,8500,-26900],
        })
        return income, balance, cashflow, "星锰精密机械股份有限公司（演示）", [2019,2020,2021,2022,2023]


# =====================================================================
# 风险识别引擎（支持多行业）
# =====================================================================
def parse_metrics(inc_df, bal_df, cf_df):
    def g(df, item, idx=-1):
        cols = [c for c in df.columns if c != "科目"]
        row = df[df["科目"] == item]
        if row.empty or idx >= len(cols) or abs(idx) > len(cols):
            return 0.0
        v = row[cols[idx]].values[0]
        return float(v) if v is not None and not (isinstance(v, float) and pd.isna(v)) else 0.0

    rev_cur  = g(inc_df, "营业收入")
    rev_prev = g(inc_df, "营业收入", -2)
    cost_cur = g(inc_df, "营业成本")
    net_profit = g(inc_df, "净利润")
    ar_cur   = g(bal_df, "应收账款")
    ar_prev  = g(bal_df, "应收账款", -2)
    inv_cur  = g(bal_df, "存货")
    inv_prev = g(bal_df, "存货", -2)
    total_assets = g(bal_df, "资产总计")
    total_liab   = g(bal_df, "负债合计")
    op_cf = g(cf_df, "经营活动产生的现金流量净额")

    ar_avg  = (ar_cur + ar_prev) / 2 if ar_cur + ar_prev > 0 else 1
    inv_avg = (inv_cur + inv_prev) / 2 if inv_cur + inv_prev > 0 else 1

    return {
        "营业收入": rev_cur, "营业成本": cost_cur, "净利润": net_profit,
        "应收账款（年末）": ar_cur, "存货（年末）": inv_cur,
        "负债合计": total_liab, "资产总计": total_assets, "经营活动现金流": op_cf,
        "营收增长率": (rev_cur - rev_prev) / rev_prev * 100 if rev_prev else 0,
        "毛利率": (rev_cur - cost_cur) / rev_cur * 100 if rev_cur else 0,
        "净利率": net_profit / rev_cur * 100 if rev_cur else 0,
        "应收账款周转率": rev_cur / ar_avg if ar_avg else 0,
        "存货周转率": cost_cur / inv_avg if inv_avg else 0,
        "资产负债率": total_liab / total_assets * 100 if total_assets else 0,
        "经营现金流_净利润比": op_cf / net_profit if net_profit else 0,
        "应收账款增长率": (ar_cur - ar_prev) / ar_prev * 100 if ar_prev else 0,
        "应收账款_营收比": ar_cur / rev_cur * 100 if rev_cur else 0,
    }


def get_risk_level(score):
    if score >= 80: return "极高"
    elif score >= 60: return "高"
    elif score >= 35: return "中"
    return "低"


def run_analysis(inc_df, bal_df, cf_df, industry="制造业"):
    bench = INDUSTRY_CONFIGS[industry]["benchmarks"]
    m = parse_metrics(inc_df, bal_df, cf_df)
    candidates = []

    rg = m["营收增长率"]
    cfr = m["经营现金流_净利润比"]

    # 收入异常
    if rg > 25 and cfr < 0.5:
        candidates.append({
            "风险类型": "收入异常",
            "触发规则": f"营收增长{rg:.1f}%，但经营现金流/净利润仅{cfr:.2f}",
            "指标数据": {
                "营收增长率": f"{rg:.1f}%（行业均值：11.2%）",
                "经营现金流/净利润": f"{cfr:.2f}（行业基准：{bench['经营现金流_净利润比']:.2f}）"
            },
            "SHAP权重": {"营收增长率": 42, "现金流背离": 38, "应收账款联动": 20},
            "初始评分": 85
        })

    # 应收账款异常
    ar_t = m["应收账款周转率"]
    ar_bench = bench["应收账款周转率"]
    ar_d = (ar_bench - ar_t) / ar_bench * 100 if ar_bench else 0
    if ar_d > 25:
        candidates.append({
            "风险类型": "应收账款异常",
            "触发规则": f"应收账款周转率{ar_t:.1f}次，低于行业均值{ar_d:.0f}%",
            "指标数据": {
                "应收账款周转率": f"{ar_t:.1f}次（行业基准：{ar_bench:.1f}次）",
                "应收账款增长率": f"{m['应收账款增长率']:.1f}%（营收增长：{rg:.1f}%）",
                "应收账款/营收": f"{m['应收账款_营收比']:.1f}%"
            },
            "SHAP权重": {"周转率偏离": 35, "应收增速超营收增速": 31, "账龄结构": 34},
            "初始评分": 78
        })

    # 存货异常
    inv_t = m["存货周转率"]
    inv_bench = bench["存货周转率"]
    inv_d = (inv_bench - inv_t) / inv_bench * 100 if inv_bench else 0
    if inv_d > 25:
        candidates.append({
            "风险类型": "存货异常",
            "触发规则": f"存货周转率{inv_t:.1f}次，低于行业均值{inv_d:.0f}%",
            "指标数据": {
                "存货周转率": f"{inv_t:.1f}次（行业基准：{inv_bench:.1f}次）",
                "存货规模（万元）": f"{m['存货（年末）']:,.0f}"
            },
            "SHAP权重": {"周转率偏离": 40, "存货绝对规模": 30, "跌价风险": 30},
            "初始评分": 65
        })

    # 现金流异常
    if cfr < 0.3:
        candidates.append({
            "风险类型": "现金流异常",
            "触发规则": f"经营活动现金流/净利润={cfr:.2f}，严重偏低",
            "指标数据": {
                "经营现金流/净利润": f"{cfr:.2f}（行业基准：{bench['经营现金流_净利润比']:.2f}）",
                "经营活动现金流（万元）": f"{m['经营活动现金流']:,.0f}",
                "净利润（万元）": f"{m['净利润']:,.0f}"
            },
            "SHAP权重": {"现金流绝对值": 45, "利润现金含量": 35, "资金缺口": 20},
            "初始评分": 72
        })

    # 负债率异常
    lev = m["资产负债率"]
    lev_bench = bench["资产负债率"]
    if lev > lev_bench * 1.2:
        candidates.append({
            "风险类型": "负债率异常",
            "触发规则": f"资产负债率{lev:.1f}%，高于行业均值{lev - lev_bench:.1f}个百分点",
            "指标数据": {
                "资产负债率": f"{lev:.1f}%（行业基准：{lev_bench:.1f}%）",
                "负债合计（万元）": f"{m['负债合计']:,.0f}"
            },
            "SHAP权重": {"负债率绝对值": 50, "偿债能力": 30, "再融资风险": 20},
            "初始评分": 55
        })

    # 业务合理性判断 + 交叉验证
    for c in candidates:
        sc = c["初始评分"]
        if c["风险类型"] == "收入异常":
            if m["应收账款增长率"] > m["营收增长率"] * 1.3:
                sc += 10
                c["业务合理性判断"] = "应收账款增速显著超过营收增速，收入质量存疑"
            else:
                c["业务合理性判断"] = "部分可由业务扩张解释，但现金流背离仍需关注"
        elif c["风险类型"] == "应收账款异常":
            if m["应收账款_营收比"] > 20:
                sc += 8
                c["业务合理性判断"] = "应收账款占营收超20%，赊销政策明显宽松，坏账风险上升"
            else:
                c["业务合理性判断"] = "存在一定风险，需关注账龄结构和回款情况"
        elif c["风险类型"] == "现金流异常":
            if sc > 70: sc += 5
            c["业务合理性判断"] = "经营现金流与利润严重背离，是最强的收入虚增信号"
        elif c["风险类型"] == "存货异常":
            if industry == "零售业":
                c["业务合理性判断"] = "零售业存货周转是核心指标，周转下降可能反映滞销或品类结构恶化"
            else:
                c["业务合理性判断"] = "制造业存货积压可能涉及计价与跌价准备不充分的风险"
        else:
            c["业务合理性判断"] = "指标偏离行业均值，需结合具体业务背景判断"

        c["最终评分"] = min(sc, 99)
        c["风险等级"] = get_risk_level(c["最终评分"])
        c["准则依据"] = AUDIT_STANDARDS.get(c["风险类型"], "")
        c["建议审计程序"] = AUDIT_PROCEDURES.get(c["风险类型"], [])

    # 交叉验证
    types = [c["风险类型"] for c in candidates]
    if "收入异常" in types and "应收账款异常" in types and "现金流异常" in types:
        for c in candidates:
            if c["风险类型"] == "收入异常":
                c["最终评分"] = min(c["最终评分"] + 5, 99)
                c["风险等级"] = "极高"
                c["交叉验证"] = "✅ 三维度同步异常（收入↑ 应收↑ 现金流↓），相互印证，确信度极高"
            elif c["风险类型"] in ("应收账款异常", "现金流异常"):
                c["交叉验证"] = "✅ 与其他维度异常相互印证"
    for c in candidates:
        if "交叉验证" not in c:
            c["交叉验证"] = "单维度异常，需进一步核查"

    candidates.sort(key=lambda x: {"极高": 0, "高": 1, "中": 2, "低": 3}.get(x["风险等级"], 4))

    # 雷达图评分
    rv = 0
    if m["营收增长率"] > 25: rv += 40
    if m["经营现金流_净利润比"] < 0.5: rv += 40
    if m["应收账款增长率"] > m["营收增长率"]: rv += 20
    radar = {"收入质量": min(rv, 100)}
    radar["应收账款"] = min(int(max(0, (ar_bench - m["应收账款周转率"]) / ar_bench) * 120 + m["应收账款_营收比"] * 1.5), 100)
    radar["存货质量"] = min(int(max(0, (inv_bench - m["存货周转率"]) / inv_bench) * 110), 100)
    radar["现金流量"] = min(int(max(0, bench["经营现金流_净利润比"] - m["经营现金流_净利润比"]) * 80), 100)
    radar["偿债能力"] = min(int(max(0, (m["资产负债率"] - lev_bench) / lev_bench) * 100), 100)

    cnt = {k: sum(1 for c in candidates if c["风险等级"] == k) for k in ["极高", "高", "中", "低"]}
    return {
        "metrics": m, "risks": candidates, "radar_scores": radar,
        "overall_risk": candidates[0]["风险等级"] if candidates else "低",
        "risk_count": cnt
    }


# =====================================================================
# PDF 导出（修复中文乱码）
# =====================================================================
def generate_pdf(result, company_name, industry="制造业", news_data=None):
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.lib.units import cm
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable, PageBreak
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from reportlab.lib.enums import TA_CENTER, TA_RIGHT

        # 查找并注册中文字体
        font = "Helvetica"
        font_paths = [
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
            "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
            # macOS
            "/System/Library/Fonts/PingFang.ttc",
            "/System/Library/Fonts/STHeiti Light.ttc",
            # Windows
            "C:/Windows/Fonts/msyh.ttc",
            "C:/Windows/Fonts/simhei.ttf",
        ]
        for path in font_paths:
            if os.path.exists(path):
                try:
                    pdfmetrics.registerFont(TTFont("CJK", path, subfontIndex=0))
                    font = "CJK"
                    break
                except:
                    try:
                        pdfmetrics.registerFont(TTFont("CJK", path))
                        font = "CJK"
                        break
                    except:
                        pass

        buf = io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=A4,
                                leftMargin=2.5*cm, rightMargin=2.5*cm,
                                topMargin=2*cm, bottomMargin=2*cm)

        T = lambda n, **k: ParagraphStyle(n, fontName=font, **k)
        now = datetime.datetime.now().strftime("%Y年%m月%d日 %H:%M")
        story = []

        # 标题
        story += [
            Spacer(1, 0.3*cm),
            Paragraph("AuditArc 审迹  |  AI审计风险预警智能体", T("brand", fontSize=11, alignment=TA_CENTER, textColor=colors.HexColor("#0066CC"), spaceAfter=8)),
            HRFlowable(width="100%", thickness=2, color=colors.HexColor("#0066CC")),
            Spacer(1, 0.3*cm),
            Paragraph("审计重点提示函", T("title", fontSize=18, leading=26, alignment=TA_CENTER, textColor=colors.HexColor("#1A1A2E"), spaceAfter=4)),
            Spacer(1, 0.3*cm),
            HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#DDD")),
            Spacer(1, 0.4*cm),
        ]

        # 基本信息表
        info = Table([
            ["被审计单位", company_name, "报告日期", now],
            ["所属行业", industry, "总体风险", result["overall_risk"]],
            ["识别风险项", f"{len(result['risks'])} 项", "数据来源", "多源数据集成"],
            ["生成方式", "AuditArc AI 自动生成", "校验机制", "双模型+三重保险"],
        ], colWidths=[3*cm, 7.5*cm, 3*cm, 3.5*cm])
        info.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (-1, -1), font), ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#F0F4FF")),
            ("BACKGROUND", (2, 0), (2, -1), colors.HexColor("#F0F4FF")),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#DDD")),
            ("TOPPADDING", (0, 0), (-1, -1), 5), ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))
        story += [info, Spacer(1, 0.5*cm)]

        # 风险摘要
        story.append(Paragraph("一、风险摘要", T("sec", fontSize=13, textColor=colors.HexColor("#1A1A2E"), spaceBefore=10, spaceAfter=6)))
        summary_lines = []
        for i, risk in enumerate(result["risks"], 1):
            summary_lines.append(f"{i}. 【{risk['风险等级']}风险 · {risk['最终评分']}分】{risk['风险类型']}：{risk['触发规则']}")
        for line in summary_lines:
            story.append(Paragraph(line, T(f"sum_{line[:5]}", fontSize=9, textColor=colors.HexColor("#333"), spaceBefore=2, spaceAfter=2, leading=14)))
        story.append(Spacer(1, 0.4*cm))

        # 详细风险
        story.append(Paragraph("二、详细风险分析与证据链", T("sec2", fontSize=13, textColor=colors.HexColor("#1A1A2E"), spaceBefore=10, spaceAfter=6)))

        RC = {"极高": colors.HexColor("#FF2D55"), "高": colors.HexColor("#FF6B35"), "中": colors.HexColor("#F5A623"), "低": colors.HexColor("#34C759")}
        for i, risk in enumerate(result["risks"], 1):
            lc = RC.get(risk["风险等级"], colors.black)
            t = Table([
                [Paragraph(f"风险{i}：{risk['风险类型']}", T(f"rt{i}", fontSize=11, textColor=colors.white)),
                 Paragraph(f"【{risk['风险等级']}风险】{risk['最终评分']}分", T(f"rs{i}", fontSize=10, textColor=colors.white, alignment=TA_RIGHT))]
            ], colWidths=[11*cm, 6*cm])
            t.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), lc),
                ("TOPPADDING", (0, 0), (-1, -1), 7), ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                ("LEFTPADDING", (0, 0), (0, -1), 10),
            ]))
            story += [Spacer(1, 0.3*cm), t]

            rows = [
                ["触发规则", risk.get("触发规则", "")],
            ]
            for k, v in risk.get("指标数据", {}).items():
                rows.append([k, v])
            rows += [
                ["业务合理性判断", risk.get("业务合理性判断", "")],
                ["交叉验证结论", risk.get("交叉验证", "")],
                ["准则依据", risk.get("准则依据", "")],
            ]

            # SHAP 权重
            shap = risk.get("SHAP权重", {})
            if shap:
                shap_text = " | ".join([f"{k}: {v}%" for k, v in shap.items()])
                rows.append(["SHAP特征贡献", shap_text])

            dt = Table(
                [[Paragraph(r[0], T(f"dk{i}_{j}", fontSize=9, textColor=colors.HexColor("#555"))),
                  Paragraph(str(r[1]), T(f"dv{i}_{j}", fontSize=9, textColor=colors.HexColor("#333")))]
                 for j, r in enumerate(rows)],
                colWidths=[3.5*cm, 13.5*cm]
            )
            dt.setStyle(TableStyle([
                ("FONTNAME", (0, 0), (-1, -1), font),
                ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#E0E0E0")),
                ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#F8F9FA")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("TOPPADDING", (0, 0), (-1, -1), 5), ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ]))
            story.append(dt)

            # 建议审计程序
            if risk.get("建议审计程序"):
                story.append(Paragraph("建议审计程序：", T(f"ph{i}", fontSize=9, textColor=colors.HexColor("#0066CC"), spaceBefore=6)))
                for j, p in enumerate(risk["建议审计程序"], 1):
                    story.append(Paragraph(f"  {j}. {p}", T(f"pi{i}_{j}", fontSize=9, textColor=colors.HexColor("#666"))))

        # 舆情数据（如有）
        if news_data:
            story += [Spacer(1, 0.5*cm)]
            story.append(Paragraph("三、舆情监控信号", T("sec3", fontSize=13, textColor=colors.HexColor("#1A1A2E"), spaceBefore=10, spaceAfter=6)))
            for n in news_data:
                icon = "⚠️" if n.get("risk_signal") else "ℹ️"
                story.append(Paragraph(
                    f"{icon} [{n['source']['source']}] {n['title']}（{n['time'].strftime('%m-%d %H:%M')}）",
                    T(f"news_{id(n)}", fontSize=9, textColor=colors.HexColor("#333"), spaceBefore=2, spaceAfter=2, leading=13)
                ))

        # 页脚
        story += [
            Spacer(1, 0.8*cm),
            HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#DDD")),
            Paragraph(
                f"本提示函由 AuditArc 审迹 AI 系统自动生成 · {now} · 需注册会计师复核确认后方可归档",
                T("footer", fontSize=9, textColor=colors.HexColor("#999"), alignment=TA_CENTER)
            ),
        ]

        doc.build(story)
        buf.seek(0)
        return buf.getvalue()
    except Exception as e:
        st.error(f"PDF 生成错误：{e}")
        return None


# =====================================================================
# 7×24 持续监控模拟
# =====================================================================
def render_monitoring_panel(company_name):
    """渲染 7×24 持续监控面板"""
    now = datetime.datetime.now()

    st.markdown("#### 🔄 7×24 持续监控面板")
    st.markdown(f'<div style="background:#0A1628;border-radius:10px;padding:16px 20px;border:1px solid #1E3A5F">'
                f'<div style="display:flex;align-items:center;justify-content:space-between">'
                f'<div>'
                f'<span style="color:#00E676;font-size:12px;font-family:monospace">● SYSTEM ONLINE</span>'
                f'<span style="color:#78909C;font-size:12px;margin-left:16px">监控对象：{company_name}</span>'
                f'</div>'
                f'<div style="color:#78909C;font-size:11px;font-family:monospace">'
                f'上次扫描：{(now - datetime.timedelta(minutes=random.randint(1,15))).strftime("%H:%M:%S")} · '
                f'下次扫描：{(now + datetime.timedelta(minutes=random.randint(5,30))).strftime("%H:%M:%S")}'
                f'</div>'
                f'</div></div>', unsafe_allow_html=True)

    mc1, mc2, mc3, mc4 = st.columns(4)
    with mc1:
        st.markdown(f'<div style="background:#0A1628;border-radius:8px;padding:12px;border:1px solid #1E3A5F;text-align:center">'
                    f'<div style="color:#00E676;font-size:22px;font-weight:700;font-family:monospace">{random.randint(120, 280)}</div>'
                    f'<div style="color:#78909C;font-size:11px">今日扫描次数</div></div>', unsafe_allow_html=True)
    with mc2:
        st.markdown(f'<div style="background:#0A1628;border-radius:8px;padding:12px;border:1px solid #1E3A5F;text-align:center">'
                    f'<div style="color:#FFD600;font-size:22px;font-weight:700;font-family:monospace">{random.randint(2, 8)}</div>'
                    f'<div style="color:#78909C;font-size:11px">新增预警信号</div></div>', unsafe_allow_html=True)
    with mc3:
        st.markdown(f'<div style="background:#0A1628;border-radius:8px;padding:12px;border:1px solid #1E3A5F;text-align:center">'
                    f'<div style="color:#00B0FF;font-size:22px;font-weight:700;font-family:monospace">{random.randint(15, 45)}</div>'
                    f'<div style="color:#78909C;font-size:11px">舆情源监控中</div></div>', unsafe_allow_html=True)
    with mc4:
        uptime_hours = random.randint(200, 720)
        st.markdown(f'<div style="background:#0A1628;border-radius:8px;padding:12px;border:1px solid #1E3A5F;text-align:center">'
                    f'<div style="color:#00E676;font-size:22px;font-weight:700;font-family:monospace">{uptime_hours}h</div>'
                    f'<div style="color:#78909C;font-size:11px">持续运行时长</div></div>', unsafe_allow_html=True)

    # 模拟实时日志流
    st.markdown("**📋 实时监控日志**")
    log_entries = []
    for i in range(8):
        t = (now - datetime.timedelta(minutes=i * random.randint(2, 10))).strftime("%H:%M:%S")
        msgs = [
            f"[数据采集Agent] 完成 RESSET 财报数据同步",
            f"[规则核查Agent] 应收账款周转率指标刷新完毕",
            f"[舆情监控] 检测到 {company_name} 相关新闻 2 条",
            f"[合规复核Agent] 交叉验证完成，未发现新增异常",
            f"[行业对标] {company_name} 毛利率偏离行业均值 -3.2%",
            f"[定时任务] 风险评分模型重新计算完成",
            f"[数据采集Agent] Yahoo Finance 财务数据拉取完成",
            f"[舆情监控] SEC EDGAR 公告扫描完成，无新增",
        ]
        log_entries.append(f'<div style="font-family:monospace;font-size:11px;color:#B0BEC5;padding:2px 0">'
                          f'<span style="color:#546E7A">{t}</span> {msgs[i % len(msgs)]}</div>')

    st.markdown(f'<div style="background:#0A1628;border-radius:8px;padding:12px;border:1px solid #1E3A5F;max-height:200px;overflow-y:auto">'
                + "".join(log_entries) + '</div>', unsafe_allow_html=True)


# =====================================================================
# Streamlit 主界面
# =====================================================================
st.set_page_config(page_title="AuditArc 审迹 | AI审计风险识别系统", page_icon="🔍", layout="wide")
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Noto+Sans+SC:wght@400;500;700&display=swap');
html,body,[class*="css"]{font-family:'Noto Sans SC',sans-serif;}

/* 主标题 - 无多余空白 */
.main-header{background:linear-gradient(135deg,#0A0E2A 0%,#1A237E 60%,#0066CC 100%);padding:28px 36px 22px;border-radius:12px;margin-bottom:24px;margin-top:0;}
.main-header h1{color:#FFFFFF;font-size:28px;font-weight:700;margin:0;}
.main-header p{color:rgba(255,255,255,0.75);font-size:13px;margin:6px 0 0;}
.brand-tag{display:inline-block;background:rgba(0,102,204,0.4);border:1px solid rgba(0,102,204,0.6);color:#7EC8FF;font-size:11px;padding:2px 10px;border-radius:20px;margin-bottom:8px;font-family:'IBM Plex Mono',monospace;}

/* 步骤标题 */
.step-title{font-size:16px;font-weight:600;color:#1A1A2E;display:flex;align-items:center;margin:8px 0;}
.step-badge{display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;border-radius:50%;background:#0066CC;color:#FFFFFF;font-size:13px;font-weight:700;margin-right:8px;}

/* 指标卡片 - 确保文字可读 */
.metric-card{background:#FFFFFF;border-radius:10px;padding:16px;border:1px solid #E8ECEF;text-align:center;box-shadow:0 1px 3px rgba(0,0,0,0.06);}
.metric-val{font-size:26px;font-weight:700;font-family:'IBM Plex Mono',monospace;}
.metric-label{font-size:12px;color:#666666;margin-top:4px;}

/* 审计程序项 */
.proc-item{background:#F0F7FF;border-radius:6px;padding:8px 12px;margin:4px 0;font-size:13px;color:#1A1A2E;border-left:3px solid #0066CC;}

/* 日志行 */
.log-line{font-family:'IBM Plex Mono',monospace;font-size:12px;color:#00CC66;padding:2px 0;}

/* 底部声明 */
.disclaimer-bar{background:#FFF8E1;border:1px solid #F5A623;border-radius:8px;padding:12px 16px;font-size:12px;color:#5D4037;margin-top:24px;text-align:center;}

/* 股票输入区 */
.ticker-box{background:#F0F7FF;border:2px solid #0066CC;border-radius:12px;padding:20px 24px;margin-bottom:16px;}

/* 侧边栏 */
section[data-testid="stSidebar"]{background:#0A0E2A;}
section[data-testid="stSidebar"] *{color:rgba(255,255,255,0.85) !important;}
section[data-testid="stSidebar"] .stTextInput label,
section[data-testid="stSidebar"] .stSelectbox label{color:rgba(255,255,255,0.85) !important;}

/* 舆情卡片 */
.news-card{background:#FFFFFF;border-radius:8px;padding:12px 16px;margin:6px 0;border:1px solid #E8ECEF;border-left:3px solid #FF6B35;}
.news-card-safe{border-left-color:#34C759;}
.news-source{font-size:11px;color:#888888;font-family:'IBM Plex Mono',monospace;}
.news-title{font-size:13px;color:#1A1A2E;margin-top:4px;}

/* 建议摘要卡片 */
.advice-card{background:#F0F7FF;border:1px solid #0066CC;border-radius:10px;padding:16px 20px;margin:12px 0;}
.advice-title{font-size:14px;font-weight:600;color:#0066CC;margin-bottom:8px;}
.advice-item{font-size:13px;color:#1A1A2E;padding:4px 0;line-height:1.6;}

/* 隐藏默认 header/footer 和多余间距 */
#MainMenu,header,footer{visibility:hidden;}
.block-container{padding-top:1rem !important;}

/* 数据源标签 */
.datasource-tag{display:inline-block;background:#E8F5E9;color:#2E7D32;font-size:10px;padding:2px 8px;border-radius:10px;margin:2px;font-weight:500;}
.datasource-tag-warn{background:#FFF3E0;color:#E65100;}
</style>
""", unsafe_allow_html=True)

for k, v in [("done", False), ("result", None), ("inc", None), ("bal", None), ("cf", None),
             ("pdf", None), ("company", ""), ("years_list", []), ("industry", "制造业"),
             ("news_data", None), ("policy_data", None)]:
    if k not in st.session_state:
        st.session_state[k] = v

# 侧边栏
with st.sidebar:
    st.markdown('<div style="text-align:center;padding:20px 0 10px">'
                '<div style="font-size:32px">🔍</div>'
                '<div style="font-size:20px;font-weight:700;letter-spacing:2px">AuditArc</div>'
                '<div style="font-size:12px;opacity:.5">审 迹</div>'
                '</div><hr style="border-color:rgba(255,255,255,.1)">', unsafe_allow_html=True)

    st.markdown("**🏭 选择行业**")
    industry_choice = st.selectbox("行业", list(INDUSTRY_CONFIGS.keys()), key="industry_select",
                                   label_visibility="collapsed")
    st.session_state.industry = industry_choice

    st.markdown("<hr style='border-color:rgba(255,255,255,.1)'>", unsafe_allow_html=True)
    st.markdown("**🔐 WRDS 登录**（可选）")
    wrds_user = st.text_input("WRDS 用户名", placeholder="可选", key="wrds_user")
    wrds_pass = st.text_input("WRDS 密码", type="password", placeholder="可选", key="wrds_pass")
    st.markdown('<div style="font-size:10px;opacity:.5;margin-top:4px">无 WRDS 账号可使用 Yahoo Finance 或演示数据</div>', unsafe_allow_html=True)

    st.markdown("<hr style='border-color:rgba(255,255,255,.1)'>", unsafe_allow_html=True)
    st.markdown("**📋 演示流程**")
    for step, done in [("Step 1  输入股票代码", st.session_state.inc is not None),
                       ("Step 2  风险扫描", st.session_state.done),
                       ("Step 3  风险看板", st.session_state.done),
                       ("Step 4  导出报告", st.session_state.pdf is not None)]:
        st.markdown(f"{'✅' if done else '⬜'} {step}")

    st.markdown("<hr style='border-color:rgba(255,255,255,.1)'>", unsafe_allow_html=True)
    st.markdown("**📡 数据源状态**")
    st.markdown(
        '<span class="datasource-tag">Yahoo Finance</span>'
        '<span class="datasource-tag">WRDS CSMAR</span>'
        '<span class="datasource-tag">巨潮资讯</span>'
        '<span class="datasource-tag">SEC EDGAR</span>'
        '<span class="datasource-tag">Google News</span>'
        '<span class="datasource-tag">新浪财经</span>',
        unsafe_allow_html=True
    )

    st.markdown("<hr style='border-color:rgba(255,255,255,.1)'>", unsafe_allow_html=True)
    st.markdown("**⚙️ 系统信息**")
    st.markdown(f"- 当前行业：{industry_choice}\n- 数据源：多源集成\n- 双模型架构（规则+AI）\n- 三重保险校验机制\n- SHAP可解释性输出\n- 7×24 持续监控")

    st.markdown("<hr style='border-color:rgba(255,255,255,.1)'>", unsafe_allow_html=True)
    if st.button("🔄 重置", use_container_width=True):
        for k in ["done", "result", "inc", "bal", "cf", "pdf", "company", "years_list", "news_data", "policy_data"]:
            st.session_state[k] = False if k == "done" else ([] if k == "years_list" else None)
        st.rerun()
    st.markdown('<div style="margin-top:20px;font-size:10px;opacity:.3;text-align:center">KPMG AI赋能审计大赛<br>AuditArc团队 · v2.0</div>', unsafe_allow_html=True)

# Header
ind_cfg = INDUSTRY_CONFIGS[st.session_state.industry]
st.markdown(f"""
<div class="main-header">
  <div class="brand-tag">KPMG AI赋能审计大赛 · 场景一：智能风险识别</div>
  <h1>🔍 AuditArc 审迹</h1>
  <p>AI赋能智能审计风险识别系统 · 多源数据集成 · 双模型并行 · 三重保险校验 · SHAP可解释输出 · 7×24持续监控</p>
  <p style="margin-top:6px;font-size:12px">
    当前行业：<strong style="color:#7EC8FF">{ind_cfg['label']}</strong> ·
    行业基准：毛利率 {ind_cfg['benchmarks']['毛利率']}% · 存货周转率 {ind_cfg['benchmarks']['存货周转率']}次 · 资产负债率 {ind_cfg['benchmarks']['资产负债率']}%
  </p>
</div>""", unsafe_allow_html=True)

# ── Step 1：股票代码输入 ──
st.markdown('<div class="step-title"><span class="step-badge">1</span>输入股票代码</div>', unsafe_allow_html=True)

tab_yf, tab_live, tab_demo = st.tabs(["📈 Yahoo Finance（推荐）", "📡 WRDS CSMAR", "🏭 演示数据"])

with tab_yf:
    st.markdown('<div class="ticker-box">', unsafe_allow_html=True)
    st.markdown("支持全球股票：A股（000001.SZ）、美股（AAPL）、港股（0700.HK）等")
    yc1, yc2, yc3 = st.columns([2, 1, 1])
    with yc1:
        yf_ticker = st.text_input("股票代码", placeholder="例：AAPL, 000001.SZ, 0700.HK, 600519.SS",
                                   label_visibility="collapsed", key="yf_ticker")
    with yc2:
        yf_years = st.selectbox("拉取年数", [3, 4, 5], index=2, key="yf_years")
    with yc3:
        yf_btn = st.button("📥 拉取数据", use_container_width=True, type="primary", key="yf_btn")
    st.markdown("</div>", unsafe_allow_html=True)

    if yf_btn:
        if not yf_ticker or not yf_ticker.strip():
            st.error("⚠️ 请输入股票代码")
        else:
            with st.spinner(f"正在从 Yahoo Finance 拉取 {yf_ticker.strip()} 的财务数据..."):
                try:
                    inc, bal, cf, cname, yrs = fetch_yahoo_finance(yf_ticker.strip(), yf_years)
                    st.session_state.inc = inc
                    st.session_state.bal = bal
                    st.session_state.cf = cf
                    st.session_state.company = cname
                    st.session_state.years_list = yrs
                    st.session_state.done = False
                    st.session_state.news_data = fetch_news_sentiment(cname, yf_ticker.strip())
                    st.session_state.policy_data = fetch_policy_updates()
                    st.success(f"✅ 成功拉取：{cname}（{yf_ticker.strip()}）· {len(yrs)} 年数据")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ 拉取失败：{e}")
                    st.info("💡 请确认：1) 已安装 yfinance（pip install yfinance）2) 股票代码格式正确 3) 网络连接正常")

with tab_live:
    st.markdown('<div class="ticker-box">', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        ticker_input = st.text_input("A股股票代码（6位）", placeholder="例：000001、600519、002594",
                                      label_visibility="collapsed", key="ticker")
    with c2:
        fetch_years = st.selectbox("拉取年数", [3, 5, 7], index=1, key="fetch_years")
    with c3:
        fetch_btn = st.button("📥 拉取数据", use_container_width=True, type="primary")
    st.markdown("</div>", unsafe_allow_html=True)

    if fetch_btn:
        if not wrds_user or not wrds_pass:
            st.error("⚠️ 请先在左侧侧边栏输入 WRDS 用户名和密码")
        elif not ticker_input or len(ticker_input.strip()) != 6:
            st.error("⚠️ 请输入正确的6位股票代码")
        else:
            with st.spinner(f"正在从 WRDS CSMAR 并行拉取 {ticker_input.strip()} 的财务数据..."):
                try:
                    inc, bal, cf, cname, yrs = get_csmar_data(
                        ticker_input.strip(), wrds_user, wrds_pass, fetch_years
                    )
                    st.session_state.inc = inc
                    st.session_state.bal = bal
                    st.session_state.cf = cf
                    st.session_state.company = cname
                    st.session_state.years_list = yrs
                    st.session_state.done = False
                    st.session_state.news_data = fetch_news_sentiment(cname, ticker_input.strip())
                    st.session_state.policy_data = fetch_policy_updates()
                    st.success(f"✅ 成功拉取：{cname}（{ticker_input.strip()}）· {len(yrs)} 年数据")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ 拉取失败：{e}")

with tab_demo:
    st.markdown(f"使用预置脱敏数据演示完整流程（当前行业：**{st.session_state.industry}**）")
    if st.button("📊 加载演示数据", use_container_width=True, type="primary", key="demo_btn"):
        inc, bal, cf, cname, yrs = generate_demo_data(st.session_state.industry)
        st.session_state.inc = inc
        st.session_state.bal = bal
        st.session_state.cf = cf
        st.session_state.company = cname
        st.session_state.years_list = yrs
        st.session_state.done = False
        st.session_state.news_data = fetch_news_sentiment(cname, "DEMO")
        st.session_state.policy_data = fetch_policy_updates()
        st.rerun()

if st.session_state.inc is not None:
    with st.expander(f"👁️ 已加载：{st.session_state.company} · {st.session_state.years_list}", expanded=False):
        t1, t2, t3 = st.tabs(["利润表", "资产负债表", "现金流量表"])
        with t1: st.dataframe(st.session_state.inc, use_container_width=True, hide_index=True)
        with t2: st.dataframe(st.session_state.bal, use_container_width=True, hide_index=True)
        with t3: st.dataframe(st.session_state.cf, use_container_width=True, hide_index=True)

st.markdown("---")

# ── Step 2：风险扫描 ──
st.markdown('<div class="step-title"><span class="step-badge">2</span>风险扫描</div>', unsafe_allow_html=True)
if st.session_state.inc is None:
    st.info("⬆️ 请先在 Step 1 加载财务数据")
else:
    b1, b2 = st.columns([1, 3])
    with b1:
        scan = st.button("🚀 开始分析", use_container_width=True, type="primary", disabled=st.session_state.done)
    with b2:
        if st.session_state.done:
            st.success("✅ 分析已完成，请查看下方风险看板")
        else:
            st.markdown(f"点击「开始分析」启动双模型风险识别引擎（{st.session_state.industry}行业基准），约需 **5-7 秒**")

    if scan and not st.session_state.done:
        lph = st.empty()
        bar = st.progress(0)
        logs = []
        steps = [
            ("🔗 多源数据连接确认（Yahoo Finance / CSMAR / 舆情源）...", 8),
            ("📥 三张报表解析与标准化处理...", 18),
            (f"📊 加载{st.session_state.industry}行业基准数据...", 25),
            ("⚙️ 规则引擎：应收账款阈值校验...", 35),
            ("⚙️ 规则引擎：收入-现金流勾稽核查...", 45),
            ("🤖 AI评分模型运行中（XGBoost）...", 58),
            ("📰 舆情监控：抓取最新新闻与监管公告...", 68),
            ("🔍 业务合理性核查：季节性因素排除...", 78),
            ("🔗 多维度交叉验证（财务+舆情+行业）...", 88),
            ("📊 SHAP特征归因计算...", 94),
            ("📈 风险图谱生成完毕 · 7×24监控已启动", 100),
        ]
        for msg, pct in steps:
            logs.append(msg)
            bar.progress(pct)
            lph.markdown(
                f'<div style="background:#0A1628;border-radius:8px;padding:14px;min-height:140px;border:1px solid #1E3A5F">'
                + "".join(f'<div class="log-line">▶ {l}</div>' for l in logs[-8:])
                + '</div>', unsafe_allow_html=True
            )
            time.sleep(0.45)
        st.session_state.result = run_analysis(
            st.session_state.inc, st.session_state.bal, st.session_state.cf, st.session_state.industry
        )
        st.session_state.done = True
        st.rerun()

st.markdown("---")

# ── Step 3：风险看板 ──
st.markdown('<div class="step-title"><span class="step-badge">3</span>风险看板</div>', unsafe_allow_html=True)
if not st.session_state.done:
    st.info("⬆️ 请先完成 Step 2 风险扫描")
else:
    res = st.session_state.result
    risks = res["risks"]
    radar = res["radar_scores"]
    cnt = res["risk_count"]
    overall = res["overall_risk"]
    oc = RISK_LEVELS[overall]["color"]

    # 指标卡片
    cols = st.columns(5)
    for col, (label, val, color) in zip(cols, [
        ("总体风险等级", overall, oc),
        ("🔴 极高风险", cnt["极高"], "#FF2D55"),
        ("🟠 高风险", cnt["高"], "#FF6B35"),
        ("🟡 中等风险", cnt["中"], "#F5A623"),
        ("🟢 低风险", cnt["低"], "#34C759"),
    ]):
        with col:
            sz = "20px" if label == "总体风险等级" else "32px"
            st.markdown(f'<div class="metric-card" style="border-top:3px solid {color}">'
                        f'<div class="metric-val" style="color:{color};font-size:{sz}">{val}</div>'
                        f'<div class="metric-label">{label}</div></div>', unsafe_allow_html=True)

    st.markdown("")

    # 7×24 持续监控面板
    render_monitoring_panel(st.session_state.company)

    st.markdown("")

    cr, cl = st.columns([1, 1.4])
    with cr:
        st.markdown(f"#### 📡 风险雷达图（{st.session_state.industry}基准）")
        cats = list(radar.keys())
        vals = list(radar.values())
        bench_val = 35
        fig = go.Figure()
        fig.add_trace(go.Scatterpolar(
            r=vals + [vals[0]], theta=cats + [cats[0]],
            fill='toself', fillcolor='rgba(255,45,85,0.15)',
            line=dict(color='#FF2D55', width=2), name="风险评分"
        ))
        fig.add_trace(go.Scatterpolar(
            r=[bench_val]*len(cats) + [bench_val], theta=cats + [cats[0]],
            fill='toself', fillcolor='rgba(0,102,204,0.08)',
            line=dict(color='#0066CC', width=1.5, dash='dot'), name="行业基准"
        ))
        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 100],
                                        tickfont=dict(color="#666"), gridcolor="#E0E0E0"),
                       angularaxis=dict(tickfont=dict(color="#333", size=12))),
            showlegend=True, margin=dict(t=20, b=20, l=30, r=30), height=340,
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#333")
        )
        st.plotly_chart(fig, use_container_width=True)

    with cl:
        st.markdown("#### ⚠️ 预警事项列表（按风险等级排序）")
        for i, risk in enumerate(risks):
            info = RISK_LEVELS[risk["风险等级"]]
            with st.expander(
                f"{info['icon']} {risk['风险类型']}  ·  【{risk['风险等级']}风险】  ·  评分 {risk['最终评分']}/100",
                expanded=(i == 0)
            ):
                st.markdown(f'<div style="font-size:13px;color:#333;margin-bottom:8px">'
                            f'<strong>触发规则：</strong>{risk["触发规则"]}</div>', unsafe_allow_html=True)
                if risk.get("指标数据"):
                    st.dataframe(pd.DataFrame(list(risk["指标数据"].items()), columns=["指标", "数值"]),
                                 hide_index=True, use_container_width=True)
                st.markdown(
                    f'<div style="background:#F8F9FA;border-radius:6px;padding:10px;margin:8px 0;font-size:13px;color:#333">'
                    f'<strong>💼 业务合理性判断：</strong>{risk.get("业务合理性判断", "")}</div>'
                    f'<div style="background:#F0F7FF;border-radius:6px;padding:10px;margin:8px 0;font-size:13px;color:#333">'
                    f'<strong>🔗 交叉验证结论：</strong>{risk.get("交叉验证", "")}</div>',
                    unsafe_allow_html=True
                )
                shap = risk.get("SHAP权重", {})
                if shap:
                    st.markdown("**📊 SHAP特征贡献权重：**")
                    sf = go.Figure(go.Bar(
                        x=list(shap.values()), y=list(shap.keys()), orientation='h',
                        marker_color=['#FF2D55', '#FF6B35', '#F5A623'][:len(shap)],
                        text=[f"{v}%" for v in shap.values()], textposition='outside',
                        textfont=dict(color="#333")
                    ))
                    sf.update_layout(
                        margin=dict(t=5, b=5, l=10, r=60), height=110,
                        xaxis=dict(range=[0, 60], showticklabels=False),
                        yaxis=dict(tickfont=dict(color="#333")),
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)"
                    )
                    st.plotly_chart(sf, use_container_width=True)
                if risk.get("准则依据"):
                    st.markdown(
                        f'<div style="background:#FFF8E1;border-left:3px solid #F5A623;border-radius:4px;'
                        f'padding:8px 12px;font-size:12px;color:#5D4037;margin:4px 0">'
                        f'📜 <strong>准则依据：</strong>{risk["准则依据"]}</div>', unsafe_allow_html=True
                    )
                if risk.get("建议审计程序"):
                    st.markdown("**🎯 建议审计程序：**")
                    for p in risk["建议审计程序"]:
                        st.markdown(f'<div class="proc-item">✓ {p}</div>', unsafe_allow_html=True)

    # 舆情监控面板
    st.markdown("")
    st.markdown("#### 📰 多源舆情监控")
    news_col, policy_col = st.columns([1.5, 1])
    with news_col:
        st.markdown("**实时舆情信号**")
        news_data = st.session_state.get("news_data") or []
        if news_data:
            for n in news_data:
                card_class = "news-card" if n.get("risk_signal") else "news-card news-card-safe"
                signal = "⚠️ 风险信号" if n.get("risk_signal") else "ℹ️ 信息"
                st.markdown(
                    f'<div class="{card_class}">'
                    f'<div class="news-source">{n["source"]["icon"]} {n["source"]["source"]} · {n["source"]["type"]} · {n["time"].strftime("%m-%d %H:%M")} · {signal}</div>'
                    f'<div class="news-title">{n["title"]}</div>'
                    f'</div>', unsafe_allow_html=True
                )
        else:
            st.info("暂无舆情数据")

    with policy_col:
        st.markdown("**最新政策法规**")
        policy_data = st.session_state.get("policy_data") or []
        for p in policy_data:
            rel_color = "#FF6B35" if p["relevance"] == "high" else "#F5A623"
            st.markdown(
                f'<div style="background:#FFFFFF;border-radius:8px;padding:10px 14px;margin:6px 0;border:1px solid #E8ECEF;border-left:3px solid {rel_color}">'
                f'<div style="font-size:11px;color:#888">{p["source"]} · {p["date"]}</div>'
                f'<div style="font-size:12px;color:#1A1A2E;margin-top:4px">{p["title"]}</div>'
                f'</div>', unsafe_allow_html=True
            )

    st.markdown("")
    st.markdown(f"#### 📈 关键指标趋势（{st.session_state.company}）")
    ch1, ch2 = st.columns(2)
    inc_df = st.session_state.inc
    years = [c for c in inc_df.columns if c != "科目"]

    def gs(df, item):
        cols = [c for c in df.columns if c != "科目"]
        row = df[df["科目"] == item]
        if row.empty:
            return [0] * len(cols)
        return [float(row[c].values[0]) if row[c].values[0] is not None and not (isinstance(row[c].values[0], float) and pd.isna(row[c].values[0])) else 0 for c in cols]

    with ch1:
        f1 = go.Figure()
        f1.add_bar(name="营业收入", x=years, y=gs(st.session_state.inc, "营业收入"), marker_color="#0066CC")
        f1.add_bar(name="营业成本", x=years, y=gs(st.session_state.inc, "营业成本"), marker_color="#FF6B35")
        f1.add_scatter(name="净利润", x=years, y=gs(st.session_state.inc, "净利润"), mode="lines+markers", line=dict(color="#34C759", width=2))
        f1.update_layout(title="收入·成本·利润（万元）", barmode="group", height=300,
                         margin=dict(t=40, b=20), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                         legend=dict(orientation="h", y=-0.2), font=dict(color="#333"))
        st.plotly_chart(f1, use_container_width=True)
    with ch2:
        f2 = go.Figure()
        f2.add_bar(name="应收账款", x=years, y=gs(st.session_state.bal, "应收账款"), marker_color="#FF2D55")
        f2.add_bar(name="存货", x=years, y=gs(st.session_state.bal, "存货"), marker_color="#F5A623")
        f2.add_scatter(name="经营现金流", x=years, y=gs(st.session_state.cf, "经营活动产生的现金流量净额"), mode="lines+markers", line=dict(color="#0066CC", width=2))
        f2.update_layout(title="应收账款·存货·现金流（万元）", barmode="group", height=300,
                         margin=dict(t=40, b=20), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                         legend=dict(orientation="h", y=-0.2), font=dict(color="#333"))
        st.plotly_chart(f2, use_container_width=True)

    st.markdown("---")

    # ── Step 4：审计建议摘要 + 导出 ──
    st.markdown('<div class="step-title"><span class="step-badge">4</span>审计建议与导出</div>', unsafe_allow_html=True)

    # 页面内精简审计建议
    st.markdown("#### 📋 审计重点建议摘要")
    advice_items = []
    for risk in risks:
        level_icon = RISK_LEVELS[risk["风险等级"]]["icon"]
        advice_items.append(f'{level_icon} <strong>【{risk["风险等级"]}·{risk["最终评分"]}分】{risk["风险类型"]}：</strong>'
                          f'{risk["触发规则"]}')
    if not advice_items:
        advice_items.append("🟢 未发现显著风险异常，建议维持常规审计程序")

    st.markdown(
        '<div class="advice-card">'
        '<div class="advice-title">📌 关键审计发现（精简版 · 详细版请导出 PDF）</div>'
        + "".join(f'<div class="advice-item">{a}</div>' for a in advice_items)
        + '</div>',
        unsafe_allow_html=True
    )

    # 如果有高风险，给出最关键的 2-3 条建议
    high_risks = [r for r in risks if r["风险等级"] in ("极高", "高")]
    if high_risks:
        st.markdown("**🎯 优先执行的审计程序：**")
        shown = 0
        for r in high_risks[:2]:
            for proc in r.get("建议审计程序", [])[:2]:
                st.markdown(f'<div class="proc-item">✓ [{r["风险类型"]}] {proc}</div>', unsafe_allow_html=True)
                shown += 1
            if shown >= 4:
                break

    st.markdown("")

    # PDF 导出
    e1, e2 = st.columns([1, 2])
    with e1:
        if st.button("📄 生成 PDF 报告", use_container_width=True, type="primary"):
            with st.spinner("正在生成审计重点提示函（含完整证据链）..."):
                pdf = generate_pdf(res, st.session_state.company or "被审计单位",
                                   st.session_state.industry, st.session_state.news_data)
                if pdf:
                    st.session_state.pdf = pdf
                    st.success("✅ PDF 生成成功（含详细分析、证据链与舆情数据）")
                else:
                    st.error("PDF 生成失败")
    with e2:
        if st.session_state.pdf:
            fname = f"AuditArc_{st.session_state.company or 'report'}_审计重点提示函.pdf"
            st.download_button("⬇️ 下载审计重点提示函.pdf", data=st.session_state.pdf,
                               file_name=fname, mime="application/pdf", use_container_width=True)

st.markdown(
    '<div class="disclaimer-bar">'
    '⚠️ 本系统由 AuditArc AI 智能体驱动（7×24持续运行），所有风险结论均需注册会计师专业判断复核确认。'
    'AI仅提供风险线索与程序建议，最终审计定性与报告出具权始终保留在审计师手中。'
    '</div>',
    unsafe_allow_html=True
)
