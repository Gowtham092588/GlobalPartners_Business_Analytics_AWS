import streamlit as st
import plotly.express as px

from athena import (
    get_athena_client,
    run_sql_query,
    get_results_df
)

from queries import get_customer_segmentation_summary_query


@st.cache_data(ttl=3600)
def load_customer_segmentation_data():

    athena = get_athena_client()

    query = get_customer_segmentation_summary_query()

    query_id = run_sql_query(
        athena,
        query
    )

    customer_df = get_results_df(
        athena,
        query_id
    )

    return customer_df


def main():

    st.set_page_config(
        page_title="Customer Segmentation",
        page_icon="👥",
        layout="wide"
    )

    # -----------------------------------------
    # Back to main page
    # -----------------------------------------

    if st.button("Back Home"):
        st.switch_page("app.py")

    st.title("Customer Segmentation Dashboard")

    st.caption(
        "Understand customer segments using recency, frequency, "
        "spending behavior, and loyalty status to support targeted marketing."
    )

    # -----------------------------------------
    # Sidebar
    # -----------------------------------------

    with st.sidebar:

        st.subheader("Segmentation Analysis")

        selected_view = st.radio(
            "Select Visualization",
            [
                "Customer Distribution",
                "RFM Behavior",
                "Loyalty Rate"
            ]
        )

        rfm_metric = None

        if selected_view == "RFM Behavior":

            rfm_metric = st.radio(
                "Select RFM Metric",
                [
                    "Recency",
                    "Frequency",
                    "Monetary"
                ]
            )

        st.divider()

        if st.button(
            "Refresh Data",
            use_container_width=True
        ):
            load_customer_segmentation_data.clear()
            st.rerun()

    # -----------------------------------------
    # Load Athena data
    # -----------------------------------------

    customer_df = load_customer_segmentation_data()

    if customer_df.empty:
        st.warning("No customer segmentation data found.")
        return

    # -----------------------------------------
    # KPI values
    # -----------------------------------------

    total_customers = int(
        customer_df["CUSTOMER_COUNT"].sum()
    )

    largest_segment_row = (
        customer_df
        .sort_values(
            "CUSTOMER_COUNT",
            ascending=False
        )
        .iloc[0]
    )

    vip_row = (
        customer_df[
            customer_df["CUSTOMER_SEGMENT"] == "VIP"
        ]
        .iloc[0]
    )

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Customers",
            f"{total_customers:,}"
        )

    with col2:
        st.metric(
            "Largest Segment",
            largest_segment_row["CUSTOMER_SEGMENT"]
        )

    with col3:
        st.metric(
            "VIP Customers",
            f"{int(vip_row['CUSTOMER_COUNT']):,}"
        )

    with col4:
        st.metric(
            "VIP Loyalty Rate",
            f"{vip_row['LOYALTY_RATE']:.1f}%"
        )

    st.divider()

    # -----------------------------------------
    # Customer Distribution
    # -----------------------------------------

    if selected_view == "Customer Distribution":

        st.subheader("Customer Segment Distribution")

        chart_df = (
            customer_df
            .sort_values(
                "CUSTOMER_COUNT",
                ascending=True
            )
        )

        fig = px.bar(
            chart_df,
            x="CUSTOMER_COUNT",
            y="CUSTOMER_SEGMENT",
            orientation="h",
            text="CUSTOMER_COUNT",
            labels={
                "CUSTOMER_COUNT": "Number of Customers",
                "CUSTOMER_SEGMENT": "Customer Segment"
            }
        )

        fig.update_traces(
            texttemplate="%{text:,.0f}",
            textposition="outside"
        )

        fig.update_layout(
            xaxis_title="Number of Customers",
            yaxis_title=None
        )

    # -----------------------------------------
    # RFM Behavior
    # -----------------------------------------

    elif selected_view == "RFM Behavior":

        if rfm_metric == "Recency":

            metric_column = "AVG_RECENCY"
            y_title = "Average Recency (Days)"
            chart_title = "Average Recency by Customer Segment"

        elif rfm_metric == "Frequency":

            metric_column = "AVG_FREQUENCY"
            y_title = "Average 90-Day Orders"
            chart_title = "Average Frequency by Customer Segment"

        else:

            metric_column = "AVG_MONETARY"
            y_title = "Average 90-Day Spend ($)"
            chart_title = "Average Monetary Value by Customer Segment"

        st.subheader(chart_title)

        chart_df = (
            customer_df
            .sort_values(
                metric_column,
                ascending=False
            )
        )

        fig = px.bar(
            chart_df,
            x="CUSTOMER_SEGMENT",
            y=metric_column,
            text=metric_column,
            labels={
                "CUSTOMER_SEGMENT": "Customer Segment",
                metric_column: y_title
            }
        )

        if rfm_metric == "Monetary":

            fig.update_traces(
                texttemplate="$%{text:.2f}",
                textposition="outside"
            )

        else:

            fig.update_traces(
                texttemplate="%{text:.2f}",
                textposition="outside"
            )

        fig.update_layout(
            xaxis_title="Customer Segment",
            yaxis_title=y_title
        )

    # -----------------------------------------
    # Loyalty Rate
    # -----------------------------------------

    elif selected_view == "Loyalty Rate":

        st.subheader("Loyalty Rate by Customer Segment")

        chart_df = (
            customer_df
            .sort_values(
                "LOYALTY_RATE",
                ascending=True
            )
        )

        fig = px.bar(
            chart_df,
            x="LOYALTY_RATE",
            y="CUSTOMER_SEGMENT",
            orientation="h",
            text="LOYALTY_RATE",
            labels={
                "LOYALTY_RATE": "Loyalty Customers (%)",
                "CUSTOMER_SEGMENT": "Customer Segment"
            }
        )

        fig.update_traces(
            texttemplate="%{text:.1f}%",
            textposition="outside"
        )

        fig.update_layout(
            xaxis_title="Loyalty Customers (%)",
            yaxis_title=None,
            xaxis=dict(
                range=[0, 100],
                ticksuffix="%"
            )
        )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


if __name__ == "__main__":
    main()
