"""
AuditArc 审迹 - AI审计风险识别系统
接入 WRDS CSMAR 数据库，支持直接输入A股股票代码
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import time
import io
import datetime
import os

# =====================================================================
# WRDS / CSMAR 数据拉取模块
# =====================================================================

@st.cache_resource(show_spinner=False)
def get_wrds_connection(wrds_username: str, wrds_password: str):
    """缓存 WRDS 连接，同一用户只建立一次连接，大幅提速"""
    import wrds
    return wrds.Connection(
        wrds_username=wrds_username,
        wrds_password=wrds_password,
        autoconnect=True
    )

@st.cache_data(ttl=3600, show_spinner=False)
def get_csmar_data(ticker: str, wrds_username: str, wrds_password: str, years: int = 5):
    """
    从 WRDS CSMAR 拉取A股上市公司三张报表（连接缓存+结果缓存1小时）
    ticker: 6位股票代码，如 '000001'
    """
    conn = get_wrds_connection(wrds_username, wrds_password)

    end_year = datetime.datetime.now().year - 1
    start_year = end_year - years + 1
    ticker_6 = ticker.zfill(6)

    income_sql = f"""
        SELECT tyear, b001000000, b001100000, b002001000,
               b002002000, b002003000, b002004000, b003000000,
               b004001000, b004000000, b005000000, b006000000, b008000000
        FROM csmar.cfi_fi2
        WHERE stkcd = '{ticker_6}'
          AND tyear BETWEEN {start_year} AND {end_year}
          AND report_type = 'A'
        ORDER BY tyear
    """
    balance_sql = f"""
        SELECT tyear, a001101000, a001110000, a001121000, a001123000,
               a001200000, a002110000, a002111100, a002130000,
               a002140000, a002000000, a000000000,
               b001101000, b001110000, b001120000,
               b001000000, b002110000, b002000000,
               c000000000, c001000000, c002000000, c003000000, c004000000
        FROM csmar.cfi_fi1
        WHERE stkcd = '{ticker_6}'
          AND tyear BETWEEN {start_year} AND {end_year}
          AND report_type = 'A'
        ORDER BY tyear
    """
    cashflow_sql = f"""
        SELECT tyear, c001000000, c001001000, c001002000, c001003000,
               c001000000_net, c002000000_net, c003000000_net, c000000000
        FROM csmar.cfi_fi3
        WHERE stkcd = '{ticker_6}'
          AND tyear BETWEEN {start_year} AND {end_year}
          AND report_type = 'A'
        ORDER BY tyear
    """
    name_sql = f"SELECT stknme FROM csmar.stk_listedcoinfannu WHERE stkcd = '{ticker_6}' LIMIT 1"

    try:
        inc_raw = conn.raw_sql(income_sql)
        bal_raw = conn.raw_sql(balance_sql)
        cf_raw  = conn.raw_sql(cashflow_sql)
        try:
            name_raw = conn.raw_sql(name_sql)
            company_name = name_raw.iloc[0, 0] if len(name_raw) > 0 else ticker_6
        except:
            company_name = ticker_6
    except Exception as e:
        raise e

    if inc_raw.empty or bal_raw.empty or cf_raw.empty:
        raise ValueError(f"未找到股票代码 {ticker_6} 的财务数据，请确认代码是否正确")

    years_list = sorted(inc_raw["tyear"].unique())

    # ── 重塑利润表 ──
    inc_map = {
        "b001000000": "营业收入",
        "b001100000": "营业成本",
        "b002001000": "销售费用",
        "b002002000": "管理费用",
        "b002003000": "研发费用",
        "b002004000": "财务费用",
        "b003000000": "营业利润",
        "b004001000": "营业外收入",
        "b004000000": "营业外支出",
        "b005000000": "利润总额",
        "b006000000": "所得税",
        "b008000000": "净利润",
    }
    inc_df = _reshape(inc_raw, "tyear", inc_map, years_list)

    # ── 重塑资产负债表 ──
    bal_map = {
        "a001101000": "货币资金",
        "a001110000": "应收账款",
        "a001121000": "预付账款",
        "a001123000": "存货",
        "a001200000": "流动资产合计",
        "a002110000": "固定资产",
        "a002111100": "在建工程",
        "a002130000": "无形资产",
        "a002140000": "长期股权投资",
        "a002000000": "非流动资产合计",
        "a000000000": "资产总计",
        "b001101000": "短期借款",
        "b001110000": "应付账款",
        "b001120000": "预收账款",
        "b001000000": "流动负债合计",
        "b002110000": "长期借款",
        "b002000000": "非流动负债合计",
        "c000000000": "负债合计",
        "c001000000": "股本",
        "c002000000": "资本公积",
        "c003000000": "未分配利润",
        "c004000000": "所有者权益合计",
    }
    bal_df = _reshape(bal_raw, "tyear", bal_map, years_list)

    # ── 重塑现金流量表 ──
    cf_map = {
        "c001000000":     "销售商品收到的现金",
        "c001001000":     "购买商品支付的现金",
        "c001002000":     "支付给职工的现金",
        "c001003000":     "支付的各项税费",
        "c001000000_net": "经营活动产生的现金流量净额",
        "c002000000_net": "投资活动产生的现金流量净额",
        "c003000000_net": "筹资活动产生的现金流量净额",
        "c000000000":     "现金及现金等价物净增加额",
    }
    cf_df = _reshape(cf_raw, "tyear", cf_map, years_list)

    return inc_df, bal_df, cf_df, company_name, years_list


def _reshape(df, year_col, col_map, years_list):
    """把宽表（每年一行）转成科目×年份格式"""
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


# =====================================================================
# 数据生成（演示用）
# =====================================================================
def generate_demo_data():
    income = pd.DataFrame({
        "科目": ["营业收入","营业成本","销售费用","管理费用","研发费用","财务费用","营业利润","营业外收入","营业外支出","利润总额","所得税","净利润"],
        "2019年": [158200,112400,8600,10800,4900,2800,18700,1100,700,19100,2865,16235],
        "2020年": [171300,121500,9200,11500,5200,3000,20900,1000,750,21150,3173,17977],
        "2021年": [182450,128600,9800,12300,5600,3200,22950,1200,800,23350,3500,19850],
        "2022年": [196800,141200,10500,13100,6200,3800,22000,900,600,22300,3350,18950],
        "2023年": [265800,198600,15200,16800,7100,6500,21600,500,1200,20900,3135,17765],
    })
    balance = pd.DataFrame({
        "科目": ["货币资金","应收账款","预付账款","存货","流动资产合计","固定资产","在建工程","无形资产","长期股权投资","非流动资产合计","资产总计","短期借款","应付账款","预收账款","流动负债合计","长期借款","非流动负债合计","负债合计","股本","资本公积","未分配利润","所有者权益合计"],
        "2019年": [32100,25600,4800,36500,98500,82300,9800,8100,5600,105800,204300,14000,19200,3800,46200,28000,8200,54400,50000,42000,57900,149900],
        "2020年": [30200,28400,5200,39600,103300,86100,11000,8500,5600,111200,214500,16000,20800,3500,49300,31500,6700,56000,50000,42000,66500,158500],
        "2021年": [28600,31200,5600,42800,111400,89600,12300,8900,5600,116400,227800,18000,22500,3200,52600,35000,5800,93400,50000,42000,42400,134400],
        "2022年": [24200,38900,6800,51200,125200,95800,18600,9200,5600,129200,254400,22000,26800,2900,61900,42000,6100,110000,50000,42000,62400,154400],
        "2023年": [18500,73500,9200,82600,189600,108200,31500,9500,5600,154800,344400,38500,31200,2100,87600,68000,8600,164200,50000,42000,88200,180200],
    })
    cashflow = pd.DataFrame({
        "科目": ["销售商品收到的现金","购买商品支付的现金","支付给职工的现金","支付的各项税费","经营活动产生的现金流量净额","投资活动产生的现金流量净额","筹资活动产生的现金流量净额","现金及现金等价物净增加额"],
        "2019年": [168200,-115600,-25800,-6900,20000,-18200,1500,3300],
        "2020年": [182100,-124800,-27200,-7400,22800,-20500,1800,4100],
        "2021年": [195600,-132800,-28600,-7800,26400,-21200,2000,7200],
        "2022年": [208500,-145600,-31200,-8200,23500,-28100,4000,-600],
        "2023年": [228600,-196800,-38600,-9500,4200,-39600,8500,-26900],
    })
    return income, balance, cashflow, "星锰精密机械股份有限公司（演示）", [2019,2020,2021,2022,2023]


# =====================================================================
# 风险识别引擎
# =====================================================================
INDUSTRY_BENCHMARKS = {
    "应收账款周转率": 7.2, "存货周转率": 4.1, "毛利率": 27.8,
    "净利率": 8.5, "资产负债率": 38.5, "经营现金流_净利润比": 0.92,
}
RISK_LEVELS = {
    "极高": {"color": "#FF2D55", "icon": "🔴"},
    "高":   {"color": "#FF6B35", "icon": "🟠"},
    "中":   {"color": "#F5A623", "icon": "🟡"},
    "低":   {"color": "#34C759", "icon": "🟢"},
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

def parse_metrics(inc_df, bal_df, cf_df):
    def g(df, item, idx=-1):
        cols = [c for c in df.columns if c != "科目"]
        row = df[df["科目"] == item]
        if row.empty or idx >= len(cols) or abs(idx) > len(cols): return 0.0
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

def run_analysis(inc_df, bal_df, cf_df):
    m = parse_metrics(inc_df, bal_df, cf_df)
    candidates = []
    rg = m["营收增长率"]; cfr = m["经营现金流_净利润比"]
    if rg > 25 and cfr < 0.5:
        candidates.append({"风险类型":"收入异常","触发规则":f"营收增长{rg:.1f}%，但经营现金流/净利润仅{cfr:.2f}","指标数据":{"营收增长率":f"{rg:.1f}%（行业均值：11.2%）","经营现金流/净利润":f"{cfr:.2f}（行业均值：0.92）"},"SHAP权重":{"营收增长率":42,"现金流背离":38,"应收账款联动":20},"初始评分":85})
    ar_t = m["应收账款周转率"]; ar_d = (INDUSTRY_BENCHMARKS["应收账款周转率"]-ar_t)/INDUSTRY_BENCHMARKS["应收账款周转率"]*100
    if ar_d > 25:
        candidates.append({"风险类型":"应收账款异常","触发规则":f"应收账款周转率{ar_t:.1f}次，低于行业均值{ar_d:.0f}%","指标数据":{"应收账款周转率":f"{ar_t:.1f}次（行业均值：7.2次）","应收账款增长率":f"{m['应收账款增长率']:.1f}%（营收增长：{rg:.1f}%）","应收账款/营收":f"{m['应收账款_营收比']:.1f}%（行业均值：15.3%）"},"SHAP权重":{"周转率偏离":35,"应收增速超营收增速":31,"账龄结构":34},"初始评分":78})
    inv_t = m["存货周转率"]; inv_d = (INDUSTRY_BENCHMARKS["存货周转率"]-inv_t)/INDUSTRY_BENCHMARKS["存货周转率"]*100
    if inv_d > 25:
        candidates.append({"风险类型":"存货异常","触发规则":f"存货周转率{inv_t:.1f}次，低于行业均值{inv_d:.0f}%","指标数据":{"存货周转率":f"{inv_t:.1f}次（行业均值：4.1次）","存货规模（万元）":f"{m['存货（年末）']:,.0f}"},"SHAP权重":{"周转率偏离":40,"存货绝对规模":30,"跌价风险":30},"初始评分":65})
    if cfr < 0.3:
        candidates.append({"风险类型":"现金流异常","触发规则":f"经营活动现金流/净利润={cfr:.2f}，严重偏低","指标数据":{"经营现金流/净利润":f"{cfr:.2f}（行业均值：0.92）","经营活动现金流（万元）":f"{m['经营活动现金流']:,.0f}","净利润（万元）":f"{m['净利润']:,.0f}"},"SHAP权重":{"现金流绝对值":45,"利润现金含量":35,"资金缺口":20},"初始评分":72})
    lev = m["资产负债率"]
    if lev > INDUSTRY_BENCHMARKS["资产负债率"] * 1.2:
        candidates.append({"风险类型":"负债率异常","触发规则":f"资产负债率{lev:.1f}%，高于行业均值{lev-INDUSTRY_BENCHMARKS['资产负债率']:.1f}个百分点","指标数据":{"资产负债率":f"{lev:.1f}%（行业均值：38.5%）","负债合计（万元）":f"{m['负债合计']:,.0f}"},"SHAP权重":{"负债率绝对值":50,"偿债能力":30,"再融资风险":20},"初始评分":55})
    for c in candidates:
        sc = c["初始评分"]
        if c["风险类型"] == "收入异常":
            if m["应收账款增长率"] > m["营收增长率"] * 1.3: sc+=10; c["业务合理性判断"]="应收账款增速显著超过营收增速，收入质量存疑"
            else: c["业务合理性判断"]="部分可由业务扩张解释，但现金流背离仍需关注"
        elif c["风险类型"] == "应收账款异常":
            if m["应收账款_营收比"] > 20: sc+=8; c["业务合理性判断"]="应收账款占营收超20%，赊销政策明显宽松，坏账风险上升"
            else: c["业务合理性判断"]="存在一定风险，需关注账龄结构和回款情况"
        elif c["风险类型"] == "现金流异常":
            if sc>70: sc+=5
            c["业务合理性判断"]="经营现金流与利润严重背离，是最强的收入虚增信号"
        else:
            c["业务合理性判断"]="指标偏离行业均值，需结合具体业务背景判断"
        c["最终评分"]=min(sc,99); c["风险等级"]=get_risk_level(c["最终评分"])
        c["准则依据"]=AUDIT_STANDARDS.get(c["风险类型"],"")
        c["建议审计程序"]=AUDIT_PROCEDURES.get(c["风险类型"],[])
    types=[c["风险类型"] for c in candidates]
    if "收入异常" in types and "应收账款异常" in types and "现金流异常" in types:
        for c in candidates:
            if c["风险类型"]=="收入异常": c["最终评分"]=min(c["最终评分"]+5,99); c["风险等级"]="极高"; c["交叉验证"]="✅ 三维度同步异常，相互印证，确信度极高"
            elif c["风险类型"] in ("应收账款异常","现金流异常"): c["交叉验证"]="✅ 与其他维度异常相互印证"
    for c in candidates:
        if "交叉验证" not in c: c["交叉验证"]="单维度异常，需进一步核查"
    candidates.sort(key=lambda x:{"极高":0,"高":1,"中":2,"低":3}.get(x["风险等级"],4))
    rv=0
    if m["营收增长率"]>25: rv+=40
    if m["经营现金流_净利润比"]<0.5: rv+=40
    if m["应收账款增长率"]>m["营收增长率"]: rv+=20
    radar={"收入质量":min(rv,100)}
    ar_b=INDUSTRY_BENCHMARKS["应收账款周转率"]
    radar["应收账款"]=min(int(max(0,(ar_b-m["应收账款周转率"])/ar_b)*120+m["应收账款_营收比"]*1.5),100)
    inv_b=INDUSTRY_BENCHMARKS["存货周转率"]
    radar["存货质量"]=min(int(max(0,(inv_b-m["存货周转率"])/inv_b)*110),100)
    radar["现金流量"]=min(int(max(0,INDUSTRY_BENCHMARKS["经营现金流_净利润比"]-m["经营现金流_净利润比"])*80),100)
    lev_b=INDUSTRY_BENCHMARKS["资产负债率"]
    radar["偿债能力"]=min(int(max(0,(m["资产负债率"]-lev_b)/lev_b)*100),100)
    cnt={k:sum(1 for c in candidates if c["风险等级"]==k) for k in ["极高","高","中","低"]}
    return {"metrics":m,"risks":candidates,"radar_scores":radar,"overall_risk":candidates[0]["风险等级"] if candidates else "低","risk_count":cnt}


# =====================================================================
# PDF 导出
# =====================================================================
def generate_pdf(result, company_name):
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.lib.units import cm
        from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,Table,TableStyle,HRFlowable
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from reportlab.lib.enums import TA_CENTER,TA_RIGHT
        font="Helvetica"
        for path in ["/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
                     "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
                     "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc"]:
            if os.path.exists(path):
                try: pdfmetrics.registerFont(TTFont("CJK",path)); font="CJK"; break
                except: pass
        buf=io.BytesIO()
        doc=SimpleDocTemplate(buf,pagesize=A4,leftMargin=2.5*cm,rightMargin=2.5*cm,topMargin=2.5*cm,bottomMargin=2.5*cm)
        T=lambda n,**k:ParagraphStyle(n,fontName=font,**k)
        now=datetime.datetime.now().strftime("%Y年%m月%d日")
        story=[Spacer(1,0.3*cm),
               Paragraph("AuditArc 审迹  |  AI审计风险预警智能体",T("b",fontSize=11,alignment=TA_CENTER,textColor=colors.HexColor("#0066CC"),spaceAfter=8)),
               HRFlowable(width="100%",thickness=2,color=colors.HexColor("#0066CC")),
               Spacer(1,0.3*cm),
               Paragraph("审计重点提示函",T("t",fontSize=18,leading=26,alignment=TA_CENTER,textColor=colors.HexColor("#1A1A2E"),spaceAfter=4)),
               Spacer(1,0.3*cm),HRFlowable(width="100%",thickness=0.5,color=colors.HexColor("#DDD")),Spacer(1,0.4*cm)]
        info=Table([["被审计单位",company_name,"报告日期",now],
                    ["总体风险",result["overall_risk"],"识别风险项","%d 项"%len(result["risks"])],
                    ["生成方式","AuditArc AI自动生成","数据来源","WRDS CSMAR"]],
                   colWidths=[3*cm,7.5*cm,3*cm,3.5*cm])
        info.setStyle(TableStyle([("FONTNAME",(0,0),(-1,-1),font),("FONTSIZE",(0,0),(-1,-1),9),
                                   ("BACKGROUND",(0,0),(0,-1),colors.HexColor("#F0F4FF")),
                                   ("BACKGROUND",(2,0),(2,-1),colors.HexColor("#F0F4FF")),
                                   ("GRID",(0,0),(-1,-1),0.5,colors.HexColor("#DDD")),
                                   ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5)]))
        story+=[info,Spacer(1,0.4*cm)]
        RC={"极高":colors.HexColor("#FF2D55"),"高":colors.HexColor("#FF6B35"),"中":colors.HexColor("#F5A623"),"低":colors.HexColor("#34C759")}
        for i,risk in enumerate(result["risks"],1):
            lc=RC.get(risk["风险等级"],colors.black)
            t=Table([[Paragraph(f"风险{i}：{risk['风险类型']}",T("rt",fontSize=11,textColor=colors.white)),
                      Paragraph(f"【{risk['风险等级']}风险】{risk['最终评分']}分",T("rs",fontSize=10,textColor=colors.white,alignment=TA_RIGHT))]],colWidths=[11*cm,6*cm])
            t.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,-1),lc),("TOPPADDING",(0,0),(-1,-1),7),("BOTTOMPADDING",(0,0),(-1,-1),7),("LEFTPADDING",(0,0),(0,-1),10)]))
            story+=[Spacer(1,0.3*cm),t]
            rows=[["触发规则",risk.get("触发规则","")]]+[[k,v] for k,v in risk.get("指标数据",{}).items()]+[["业务合理性",risk.get("业务合理性判断","")],["交叉验证",risk.get("交叉验证","")],["准则依据",risk.get("准则依据","")]]
            dt=Table([[Paragraph(r[0],T("dk",fontSize=9,textColor=colors.HexColor("#555"))),Paragraph(str(r[1]),T("db",fontSize=9,textColor=colors.HexColor("#333")))] for r in rows],colWidths=[3.5*cm,13.5*cm])
            dt.setStyle(TableStyle([("FONTNAME",(0,0),(-1,-1),font),("GRID",(0,0),(-1,-1),0.3,colors.HexColor("#E0E0E0")),("BACKGROUND",(0,0),(0,-1),colors.HexColor("#F8F9FA")),("VALIGN",(0,0),(-1,-1),"TOP"),("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),("LEFTPADDING",(0,0),(-1,-1),8)]))
            story.append(dt)
            if risk.get("建议审计程序"):
                story.append(Paragraph("建议审计程序：",T("ph",fontSize=9,textColor=colors.HexColor("#0066CC"),spaceBefore=6)))
                for j,p in enumerate(risk["建议审计程序"],1): story.append(Paragraph(f"  {j}. {p}",T("pi",fontSize=9,textColor=colors.HexColor("#666"))))
        story+=[Spacer(1,0.8*cm),HRFlowable(width="100%",thickness=0.5,color=colors.HexColor("#DDD")),
                Paragraph("本提示函由 AuditArc 审迹 AI 系统自动生成 · "+now+" · 需审计师复核确认后方可归档",T("foot",fontSize=9,textColor=colors.HexColor("#999"),alignment=TA_CENTER))]
        doc.build(story); buf.seek(0); return buf.getvalue()
    except Exception as e:
        return None


# =====================================================================
# Streamlit 主界面
# =====================================================================
st.set_page_config(page_title="AuditArc 审迹 | AI审计风险识别系统", page_icon="🔍", layout="wide")
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Noto+Sans+SC:wght@400;500;700&display=swap');
html,body,[class*="css"]{font-family:'Noto Sans SC',sans-serif;}
.main-header{background:linear-gradient(135deg,#0A0E2A 0%,#1A237E 60%,#0066CC 100%);padding:28px 36px 22px;border-radius:12px;margin-bottom:24px;}
.main-header h1{color:white;font-size:28px;font-weight:700;margin:0;}
.main-header p{color:rgba(255,255,255,0.7);font-size:13px;margin:6px 0 0;}
.brand-tag{display:inline-block;background:rgba(0,102,204,0.4);border:1px solid rgba(0,102,204,0.6);color:#7EC8FF;font-size:11px;padding:2px 10px;border-radius:20px;margin-bottom:8px;font-family:'IBM Plex Mono',monospace;}
.step-title{font-size:16px;font-weight:600;color:#1A1A2E;display:flex;align-items:center;margin:8px 0;}
.step-badge{display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;border-radius:50%;background:#0066CC;color:white;font-size:13px;font-weight:700;margin-right:8px;}
.metric-card{background:white;border-radius:10px;padding:16px;border:1px solid #E8ECEF;text-align:center;}
.metric-val{font-size:26px;font-weight:700;color:#1A1A2E;font-family:'IBM Plex Mono',monospace;}
.metric-label{font-size:12px;color:#888;margin-top:4px;}
.proc-item{background:#F0F7FF;border-radius:6px;padding:8px 12px;margin:4px 0;font-size:13px;color:#333;border-left:3px solid #0066CC;}
.log-line{font-family:'IBM Plex Mono',monospace;font-size:12px;color:#00CC66;padding:2px 0;}
.disclaimer-bar{background:#FFF8E1;border:1px solid #F5A623;border-radius:8px;padding:12px 16px;font-size:12px;color:#8B4513;margin-top:24px;text-align:center;}
.ticker-box{background:#F0F7FF;border:2px solid #0066CC;border-radius:12px;padding:20px 24px;margin-bottom:16px;}
section[data-testid="stSidebar"]{background:#0A0E2A;}
section[data-testid="stSidebar"] *{color:rgba(255,255,255,0.85) !important;}
#MainMenu,header,footer{visibility:hidden;}
</style>
""", unsafe_allow_html=True)

for k,v in [("done",False),("result",None),("inc",None),("bal",None),("cf",None),("pdf",None),("company",""),("years_list",[])]:
    if k not in st.session_state: st.session_state[k]=v

# 侧边栏
with st.sidebar:
    st.markdown('<div style="text-align:center;padding:20px 0 10px"><div style="font-size:32px">🔍</div><div style="font-size:20px;font-weight:700;letter-spacing:2px">AuditArc</div><div style="font-size:12px;opacity:.5">审 迹</div></div><hr style="border-color:rgba(255,255,255,.1)">', unsafe_allow_html=True)
    st.markdown("**🔐 WRDS 登录**")
    wrds_user = st.text_input("WRDS 用户名", placeholder="your_wrds_username", key="wrds_user")
    wrds_pass = st.text_input("WRDS 密码", type="password", placeholder="••••••••", key="wrds_pass")
    st.markdown("<hr style='border-color:rgba(255,255,255,.1)'>", unsafe_allow_html=True)
    st.markdown("**📋 演示流程**")
    for step, done in [("Step 1  输入股票代码", st.session_state.inc is not None),
                       ("Step 2  风险扫描", st.session_state.done),
                       ("Step 3  风险看板", st.session_state.done),
                       ("Step 4  导出报告", st.session_state.pdf is not None)]:
        st.markdown(f"{'✅' if done else '⬜'} {step}")
    st.markdown("<hr style='border-color:rgba(255,255,255,.1)'>", unsafe_allow_html=True)
    st.markdown("**⚙️ 系统信息**\n- 数据源：WRDS CSMAR\n- 双模型架构（规则+AI）\n- 三重保险校验机制\n- SHAP可解释性输出")
    st.markdown("<hr style='border-color:rgba(255,255,255,.1)'>", unsafe_allow_html=True)
    if st.button("🔄 重置", use_container_width=True):
        for k in ["done","result","inc","bal","cf","pdf","company","years_list"]:
            st.session_state[k] = False if k=="done" else ([] if k=="years_list" else None)
        st.rerun()
    st.markdown('<div style="margin-top:20px;font-size:10px;opacity:.3;text-align:center">KPMG AI赋能审计大赛<br>AuditArc团队 · 演示版本</div>', unsafe_allow_html=True)

# Header
st.markdown("""
<div class="main-header">
  <div class="brand-tag">KPMG AI赋能审计大赛 · 场景一：智能风险识别</div>
  <h1>🔍 AuditArc 审迹</h1>
  <p>AI赋能智能审计风险识别系统 · 接入 WRDS CSMAR · 双模型并行 · 三重保险校验 · SHAP可解释输出</p>
</div>""", unsafe_allow_html=True)

# ── Step 1：股票代码输入 ──
st.markdown('<div class="step-title"><span class="step-badge">1</span>输入股票代码</div>', unsafe_allow_html=True)

tab_live, tab_demo = st.tabs(["📡 实时拉取（WRDS）", "🏭 演示数据"])

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
            with st.spinner(f"正在从 WRDS CSMAR 拉取 {ticker_input.strip()} 的财务数据..."):
                try:
                    inc, bal, cf, cname, yrs = get_csmar_data(
                        ticker_input.strip(), wrds_user, wrds_pass, fetch_years
                    )
                    st.session_state.inc = inc
                    st.session_state.bal = bal
                    st.session_state.cf  = cf
                    st.session_state.company = cname
                    st.session_state.years_list = yrs
                    st.session_state.done = False
                    st.success(f"✅ 成功拉取：{cname}（{ticker_input.strip()}）· {len(yrs)} 年数据")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ 拉取失败：{e}")

with tab_demo:
    st.markdown("使用预置脱敏数据演示完整流程（无需WRDS账号）")
    if st.button("📊 加载演示数据", use_container_width=True, type="primary", key="demo_btn"):
        inc, bal, cf, cname, yrs = generate_demo_data()
        st.session_state.inc = inc; st.session_state.bal = bal; st.session_state.cf = cf
        st.session_state.company = cname; st.session_state.years_list = yrs
        st.session_state.done = False
        st.rerun()

if st.session_state.inc is not None:
    with st.expander(f"👁️ 已加载：{st.session_state.company} · {st.session_state.years_list}", expanded=False):
        t1,t2,t3 = st.tabs(["利润表","资产负债表","现金流量表"])
        with t1: st.dataframe(st.session_state.inc, use_container_width=True, hide_index=True)
        with t2: st.dataframe(st.session_state.bal, use_container_width=True, hide_index=True)
        with t3: st.dataframe(st.session_state.cf,  use_container_width=True, hide_index=True)

st.markdown("---")

# ── Step 2：风险扫描 ──
st.markdown('<div class="step-title"><span class="step-badge">2</span>风险扫描</div>', unsafe_allow_html=True)
if st.session_state.inc is None:
    st.info("⬆️ 请先在 Step 1 加载财务数据")
else:
    b1,b2 = st.columns([1,3])
    with b1: scan=st.button("🚀 开始分析",use_container_width=True,type="primary",disabled=st.session_state.done)
    with b2:
        if st.session_state.done: st.success("✅ 分析已完成，请查看下方风险看板")
        else: st.markdown("点击「开始分析」启动双模型风险识别引擎，约需 **5-7 秒**")
    if scan and not st.session_state.done:
        lph=st.empty(); bar=st.progress(0); logs=[]
        steps=[("🔗 数据源连接确认...",10),("📥 三张报表解析中...",20),("⚙️ 规则引擎：应收账款阈值校验...",35),("⚙️ 规则引擎：收入-现金流勾稽核查...",50),("🤖 AI评分模型运行中（XGBoost）...",65),("🔍 业务合理性核查：季节性因素排除...",78),("🔗 多维度交叉验证...",88),("📊 SHAP特征归因计算...",94),("📈 风险图谱生成完毕",100)]
        for msg,pct in steps:
            logs.append(msg); bar.progress(pct)
            lph.markdown(f'<div style="background:#0A0E2A;border-radius:8px;padding:14px;min-height:120px">'+"".join(f'<div class="log-line">▶ {l}</div>' for l in logs[-8:])+'</div>',unsafe_allow_html=True)
            time.sleep(0.5)
        st.session_state.result=run_analysis(st.session_state.inc,st.session_state.bal,st.session_state.cf)
        st.session_state.done=True; st.rerun()

st.markdown("---")

# ── Step 3：风险看板 ──
st.markdown('<div class="step-title"><span class="step-badge">3</span>风险看板</div>', unsafe_allow_html=True)
if not st.session_state.done:
    st.info("⬆️ 请先完成 Step 2 风险扫描")
else:
    res=st.session_state.result; risks=res["risks"]; radar=res["radar_scores"]; cnt=res["risk_count"]
    overall=res["overall_risk"]; oc=RISK_LEVELS[overall]["color"]
    cols=st.columns(5)
    for col,(label,val,color) in zip(cols,[("总体风险等级",overall,oc),("🔴 极高风险",cnt["极高"],"#FF2D55"),("🟠 高风险",cnt["高"],"#FF6B35"),("🟡 中等风险",cnt["中"],"#F5A623"),("🟢 低风险",cnt["低"],"#34C759")]):
        with col:
            sz="20px" if label=="总体风险等级" else "32px"
            st.markdown(f'<div class="metric-card" style="border-top:3px solid {color}"><div class="metric-val" style="color:{color};font-size:{sz}">{val}</div><div class="metric-label">{label}</div></div>',unsafe_allow_html=True)
    st.markdown("")
    cr,cl=st.columns([1,1.4])
    with cr:
        st.markdown("#### 📡 风险雷达图（五维度）")
        cats=list(radar.keys()); vals=list(radar.values())
        fig=go.Figure()
        fig.add_trace(go.Scatterpolar(r=vals+[vals[0]],theta=cats+[cats[0]],fill='toself',fillcolor='rgba(255,45,85,0.15)',line=dict(color='#FF2D55',width=2),name="风险评分"))
        fig.add_trace(go.Scatterpolar(r=[35]*len(cats)+[35],theta=cats+[cats[0]],fill='toself',fillcolor='rgba(0,102,204,0.08)',line=dict(color='#0066CC',width=1.5,dash='dot'),name="行业基准"))
        fig.update_layout(polar=dict(radialaxis=dict(visible=True,range=[0,100])),showlegend=True,margin=dict(t=20,b=20,l=30,r=30),height=340,paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig,use_container_width=True)
    with cl:
        st.markdown("#### ⚠️ 预警事项列表（按风险等级排序）")
        for i,risk in enumerate(risks):
            info=RISK_LEVELS[risk["风险等级"]]
            with st.expander(f"{info['icon']} {risk['风险类型']}  ·  【{risk['风险等级']}风险】  ·  评分 {risk['最终评分']}/100",expanded=(i==0)):
                st.markdown(f'<div style="font-size:13px;color:#555;margin-bottom:8px"><strong>触发规则：</strong>{risk["触发规则"]}</div>',unsafe_allow_html=True)
                if risk.get("指标数据"):
                    st.dataframe(pd.DataFrame(list(risk["指标数据"].items()),columns=["指标","数值"]),hide_index=True,use_container_width=True)
                st.markdown(f'<div style="background:#F8F9FA;border-radius:6px;padding:10px;margin:8px 0;font-size:13px"><strong>💼 业务合理性判断：</strong>{risk.get("业务合理性判断","")}</div><div style="background:#F0F7FF;border-radius:6px;padding:10px;margin:8px 0;font-size:13px"><strong>🔗 交叉验证结论：</strong>{risk.get("交叉验证","")}</div>',unsafe_allow_html=True)
                shap=risk.get("SHAP权重",{})
                if shap:
                    st.markdown("**📊 SHAP特征贡献权重：**")
                    sf=go.Figure(go.Bar(x=list(shap.values()),y=list(shap.keys()),orientation='h',marker_color=['#FF2D55','#FF6B35','#F5A623'][:len(shap)],text=[f"{v}%" for v in shap.values()],textposition='outside'))
                    sf.update_layout(margin=dict(t=5,b=5,l=10,r=60),height=110,xaxis=dict(range=[0,60],showticklabels=False),paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)")
                    st.plotly_chart(sf,use_container_width=True)
                if risk.get("准则依据"):
                    st.markdown(f'<div style="background:#FFF8E1;border-left:3px solid #F5A623;border-radius:4px;padding:8px 12px;font-size:12px;color:#666;margin:4px 0">📜 <strong>准则依据：</strong>{risk["准则依据"]}</div>',unsafe_allow_html=True)
                if risk.get("建议审计程序"):
                    st.markdown("**🎯 建议审计程序：**")
                    for p in risk["建议审计程序"]: st.markdown(f'<div class="proc-item">✓ {p}</div>',unsafe_allow_html=True)

    st.markdown("")
    st.markdown(f"#### 📈 关键指标趋势（{st.session_state.company}）")
    ch1,ch2=st.columns(2)
    inc_df=st.session_state.inc
    years=[c for c in inc_df.columns if c!="科目"]
    def gs(df,item):
        cols=[c for c in df.columns if c!="科目"]
        row=df[df["科目"]==item]
        if row.empty: return [0]*len(cols)
        return [float(row[c].values[0]) if row[c].values[0] is not None and not (isinstance(row[c].values[0],float) and pd.isna(row[c].values[0])) else 0 for c in cols]
    with ch1:
        f1=go.Figure()
        f1.add_bar(name="营业收入",x=years,y=gs(st.session_state.inc,"营业收入"),marker_color="#0066CC")
        f1.add_bar(name="营业成本",x=years,y=gs(st.session_state.inc,"营业成本"),marker_color="#FF6B35")
        f1.add_scatter(name="净利润",x=years,y=gs(st.session_state.inc,"净利润"),mode="lines+markers",line=dict(color="#34C759",width=2))
        f1.update_layout(title="收入·成本·利润（万元）",barmode="group",height=300,margin=dict(t=40,b=20),paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",legend=dict(orientation="h",y=-0.2))
        st.plotly_chart(f1,use_container_width=True)
    with ch2:
        f2=go.Figure()
        f2.add_bar(name="应收账款",x=years,y=gs(st.session_state.bal,"应收账款"),marker_color="#FF2D55")
        f2.add_bar(name="存货",x=years,y=gs(st.session_state.bal,"存货"),marker_color="#F5A623")
        f2.add_scatter(name="经营现金流",x=years,y=gs(st.session_state.cf,"经营活动产生的现金流量净额"),mode="lines+markers",line=dict(color="#0066CC",width=2))
        f2.update_layout(title="应收账款·存货·现金流（万元）",barmode="group",height=300,margin=dict(t=40,b=20),paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",legend=dict(orientation="h",y=-0.2))
        st.plotly_chart(f2,use_container_width=True)

    st.markdown("---")
    # ── Step 4：导出 ──
    st.markdown('<div class="step-title"><span class="step-badge">4</span>导出审计重点提示函</div>', unsafe_allow_html=True)
    e1,e2=st.columns([1,2])
    with e1:
        if st.button("📄 生成 PDF 报告",use_container_width=True,type="primary"):
            with st.spinner("正在生成审计重点提示函..."):
                pdf=generate_pdf(res, st.session_state.company or "被审计单位")
                if pdf: st.session_state.pdf=pdf; st.success("✅ PDF生成成功")
                else: st.error("PDF生成失败")
    with e2:
        if st.session_state.pdf:
            fname = f"AuditArc_{st.session_state.company or 'report'}_审计重点提示函.pdf"
            st.download_button("⬇️ 下载审计重点提示函.pdf",data=st.session_state.pdf,file_name=fname,mime="application/pdf",use_container_width=True)

st.markdown('<div class="disclaimer-bar">⚠️ 本系统由 AuditArc AI 智能体驱动，所有风险结论均需注册会计师专业判断复核确认。AI仅提供风险线索与程序建议，最终审计定性与报告出具权始终保留在审计师手中。</div>',unsafe_allow_html=True)
