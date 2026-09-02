import streamlit as st
import plotly.express as px
import pandas as pd
from athena import (
    get_athena_client,
    run_sql_query,
    get_results_df
)
from queries import get_churn_risk_query
from metrics import calculate_churn_kpis


@st.cache_data(ttl=3600)
def load_churn_data():

    athena = get_athena_client()

    query = get_churn_risk_query()

    query_id = run_sql_query(
        athena,
        query
    )

    churn_df = get_results_df(
        athena,
        query_id
    )

    return churn_df


def main():

    st.set_page_config(
        page_title="Churn Risk Indicators",
        page_icon="⚠️",
        layout="wide"
    )

    if st.button("Back to Home"):
        st.switch_page("app.py")

    st.title("⚠️Churn Risk Indicators Dashboard")

    st.caption(
        "Customers become classified as at risk once inactivity exceeds "
        "90 days. The chart shows a clear threshold at 90 days, making "
        "days since last order the primary churn-risk indicator in the current model."
    )

    churn_df = load_churn_data()

    if churn_df.empty:
        st.warning("No churn data found.")
        return

    kpis = calculate_churn_kpis(churn_df)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Customers At Risk",
            value=f"{kpis['at_risk_customers']:,}"
        )

    with col2:
        st.metric(
            label="Churn Risk Rate",
            value=f"{kpis['churn_risk_rate']:.1f}%"
        )

    with col3:
        st.metric(
            label="Avg Days Since Last Order",
            value=f"{kpis['avg_days_since_last_order']:.2f}"
        )

    with col4:
        st.metric(
            label="Customers With Measurable Spend Decline",
            value=f"{kpis['declining_spend_customers']:,}"
        )

    churn_chart_df = churn_df.copy()

    churn_chart_df["INACTIVITY_RANGE"] = pd.cut(
        churn_chart_df["DAYS_SINCE_LAST_ORDER"],
        bins=[0, 30, 60, 90, 180, 365, float("inf")],
        labels=[
            "0-30 days",
            "31-60 days",
            "61-90 days",
            "91-180 days",
            "181-365 days",
            "365+ days"
        ],
        include_lowest=True
    )

    churn_chart_df["IS_AT_RISK"] = (
        churn_chart_df["CHURN_STATUS"] == "AT_RISK"
    )

    churn_risk_df = (
        churn_chart_df
        .groupby(
            "INACTIVITY_RANGE",
            observed=True,
            as_index=False
        )
        .agg(
            CUSTOMER_COUNT=("USER_ID", "nunique"),
            CHURN_RISK_RATE=("IS_AT_RISK", "mean")
        )
    )

    churn_risk_df["CHURN_RISK_RATE"] = (
        churn_risk_df["CHURN_RISK_RATE"] * 100
    )

    st.subheader("Churn Risk by Days Since Last Order")

    fig = px.bar(
        churn_risk_df,
        x="INACTIVITY_RANGE",
        y="CHURN_RISK_RATE",
        text="CHURN_RISK_RATE",
        labels={
            "INACTIVITY_RANGE": "Days Since Last Order",
            "CHURN_RISK_RATE": "Customers At Risk (%)"
        }
    )

    fig.update_traces(
        texttemplate="%{text:.1f}%",
        textposition="outside"
    )

    fig.update_layout(
        yaxis=dict(
            range=[0, 100],
            ticksuffix="%"
        ),
        xaxis_title="Days Since Last Order",
        yaxis_title="Customers At Risk (%)"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


if __name__ == "__main__":
    main()
