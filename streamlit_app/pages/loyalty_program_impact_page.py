import streamlit as st
import plotly.express as px
import pandas as pd
from athena import (
    get_athena_client,
    run_sql_query,
    get_results_df
)
from queries import get_loyalty_impact_query


@st.cache_data(ttl=3600)
def load_loyalty_impact_data():

    athena = get_athena_client()

    query = get_loyalty_impact_query()

    query_id = run_sql_query(
        athena,
        query
    )

    loyalty_df = get_results_df(
        athena,
        query_id
    )

    return loyalty_df


def main():

    st.set_page_config(
        page_title="Loyalty Program Impact",
        page_icon="⭐",
        layout="wide"
    )

    if st.button("Back to Home"):
        st.switch_page("app.py")

    st.title("⭐Loyalty Program Impact Dashboard")

    st.caption(
        "Compare loyalty and non-loyalty customers across "
        "spending, repeat behavior, and lifetime value."
    )

    with st.sidebar:

        st.subheader("Loyalty Analysis")

        selected_metric = st.radio(
            "Select Comparison",
            [
                "Average Order Value",
                "Average Lifetime Orders",
                "Repeat Customer Rate",
                "Average CLV"
            ]
        )

        st.divider()

        if st.button(
            "Refresh Data",
            use_container_width=True
        ):
            load_loyalty_impact_data.clear()
            st.rerun()

        st.caption(
            "Reloads the latest loyalty metrics from Athena."
        )

    loyalty_df = load_loyalty_impact_data()

    if loyalty_df.empty:
        st.warning("No loyalty data found.")
        return

    loyalty_row = (
        loyalty_df[
            loyalty_df["IS_LOYALTY"] == True
        ]
        .iloc[0]
    )

    non_loyalty_row = (
        loyalty_df[
            loyalty_df["IS_LOYALTY"] == False
        ]
        .iloc[0]
    )

    st.subheader("Loyalty vs Non-Loyalty")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Average Order Value",
            f"${loyalty_row['AVG_ORDER_VALUE']:.2f}",
            delta=(
                f"${loyalty_row['AVG_ORDER_VALUE'] - non_loyalty_row['AVG_ORDER_VALUE']:.2f} "
                "vs non-loyalty"
            )
        )

    with col2:
        st.metric(
            "Average Lifetime Orders",
            f"{loyalty_row['AVG_LIFETIME_ORDERS']:.2f}",
            delta=(
                f"{loyalty_row['AVG_LIFETIME_ORDERS'] - non_loyalty_row['AVG_LIFETIME_ORDERS']:.2f} "
                "vs non-loyalty"
            )
        )

    with col3:
        st.metric(
            "Repeat Customer Rate",
            f"{loyalty_row['REPEAT_CUSTOMER_RATE']:.1f}%",
            delta=(
                f"{loyalty_row['REPEAT_CUSTOMER_RATE'] - non_loyalty_row['REPEAT_CUSTOMER_RATE']:.1f}% "
                "vs non-loyalty"
            )
        )

    with col4:
        st.metric(
            "Average CLV",
            f"${loyalty_row['AVG_CLV']:.2f}",
            delta=(
                f"${loyalty_row['AVG_CLV'] - non_loyalty_row['AVG_CLV']:.2f} "
                "vs non-loyalty"
            )
        )

    st.divider()
    st.subheader(selected_metric)

    if selected_metric == "Average Order Value":

        chart_df = pd.DataFrame({
            "LOYALTY_STATUS": [
                "Loyalty",
                "Non-Loyalty"
            ],
            "VALUE": [
                loyalty_row["AVG_ORDER_VALUE"],
                non_loyalty_row["AVG_ORDER_VALUE"]
            ]
        })

        fig = px.bar(
            chart_df,
            x="LOYALTY_STATUS",
            y="VALUE",
            text="VALUE",
            labels={
                "LOYALTY_STATUS": "Loyalty Status",
                "VALUE": "Average Order Value ($)"
            }
        )

        fig.update_traces(
            texttemplate="$%{text:.2f}",
            textposition="outside"
        )

        fig.update_layout(
            xaxis_title=None,
            yaxis_title="Average Order Value ($)"
        )

    elif selected_metric == "Average Lifetime Orders":

        chart_df = pd.DataFrame({
            "LOYALTY_STATUS": [
                "Loyalty",
                "Non-Loyalty"
            ],
            "VALUE": [
                loyalty_row["AVG_LIFETIME_ORDERS"],
                non_loyalty_row["AVG_LIFETIME_ORDERS"]
            ]
        })

        fig = px.bar(
            chart_df,
            x="LOYALTY_STATUS",
            y="VALUE",
            text="VALUE",
            labels={
                "LOYALTY_STATUS": "Loyalty Status",
                "VALUE": "Average Lifetime Orders"
            }
        )

        fig.update_traces(
            texttemplate="%{text:.2f}",
            textposition="outside"
        )

        fig.update_layout(
            xaxis_title=None,
            yaxis_title="Average Lifetime Orders"
        )

    elif selected_metric == "Repeat Customer Rate":

        chart_df = pd.DataFrame({
            "LOYALTY_STATUS": [
                "Loyalty",
                "Non-Loyalty"
            ],
            "VALUE": [
                loyalty_row["REPEAT_CUSTOMER_RATE"],
                non_loyalty_row["REPEAT_CUSTOMER_RATE"]
            ]
        })

        fig = px.bar(
            chart_df,
            x="LOYALTY_STATUS",
            y="VALUE",
            text="VALUE",
            labels={
                "LOYALTY_STATUS": "Loyalty Status",
                "VALUE": "Repeat Customer Rate (%)"
            }
        )

        fig.update_traces(
            texttemplate="%{text:.1f}%",
            textposition="outside"
        )

        fig.update_layout(
            xaxis_title=None,
            yaxis_title="Repeat Customer Rate (%)",
            yaxis=dict(
                range=[0, 100],
                ticksuffix="%"
            )
        )

    elif selected_metric == "Average CLV":

        chart_df = pd.DataFrame({
            "LOYALTY_STATUS": [
                "Loyalty",
                "Non-Loyalty"
            ],
            "VALUE": [
                loyalty_row["AVG_CLV"],
                non_loyalty_row["AVG_CLV"]
            ]
        })

        fig = px.bar(
            chart_df,
            x="LOYALTY_STATUS",
            y="VALUE",
            text="VALUE",
            labels={
                "LOYALTY_STATUS": "Loyalty Status",
                "VALUE": "Average CLV ($)"
            }
        )

        fig.update_traces(
            texttemplate="$%{text:.2f}",
            textposition="outside"
        )

        fig.update_layout(
            xaxis_title=None,
            yaxis_title="Average CLV ($)"
        )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


if __name__ == "__main__":
    main()
