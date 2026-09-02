import streamlit as st
import plotly.express as px
import pandas as pd

from athena import (
    get_athena_client,
    run_sql_query,
    get_results_df
)

from queries import get_location_performance_query


@st.cache_data(ttl=3600)
def load_location_performance_data():

    athena = get_athena_client()

    query = get_location_performance_query()

    query_id = run_sql_query(
        athena,
        query
    )

    location_df = get_results_df(
        athena,
        query_id
    )

    return location_df


def main():

    st.set_page_config(
        page_title="Location Performance",
        page_icon="📍",
        layout="wide"
    )

    if st.button("Back to Home"):
        st.switch_page("app.py")

    st.title("📍Location Performance Dashboard")

    st.caption(
        "Compare restaurant locations by revenue, order volume, "
        "average order value, and order activity."
    )

    # -----------------------------------------
    # Sidebar
    # -----------------------------------------

    with st.sidebar:

        st.subheader("Location Analysis")

        selected_view = st.radio(
            "Select Visualization",
            [
                "Revenue Ranking",
                "Order Volume",
                "Average Order Value",
                "Order Activity"
            ]
        )

        st.divider()

        if st.button(
            "Refresh Data",
            use_container_width=True
        ):
            load_location_performance_data.clear()
            st.rerun()

        st.caption(
            "Reloads the latest location metrics from Athena."
        )

    # -----------------------------------------
    # Load data
    # -----------------------------------------

    location_df = load_location_performance_data()

    if location_df.empty:
        st.warning("No location data found.")
        return

    # Create readable location labels
    location_df["LOCATION_LABEL"] = (
        "Location "
        + location_df["REVENUE_RANK"].astype(int).astype(str)
    )

    top_location = (
        location_df
        .sort_values("REVENUE_RANK")
        .iloc[0]
    )

    bottom_location = (
        location_df
        .sort_values("REVENUE_RANK")
        .iloc[-1]
    )

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Locations",
            f"{location_df['RESTAURANT_ID'].nunique():,}"
        )

    with col2:
        st.metric(
            "Top Location Revenue",
            f"${top_location['TOTAL_REVENUE']:,.2f}"
        )

    with col3:
        st.metric(
            "Top Location Orders",
            f"{top_location['TOTAL_ORDERS']:,.0f}"
        )

    with col4:
        st.metric(
            "Top Location AOV",
            f"${top_location['AVG_ORDER_VALUE']:,.2f}"
        )

    st.divider()

    if selected_view == "Revenue Ranking":

        st.subheader("Location Revenue Ranking")

        chart_df = (
            location_df
            .sort_values(
                "TOTAL_REVENUE",
                ascending=True
            )
        )

        fig = px.bar(
            chart_df,
            x="TOTAL_REVENUE",
            y="LOCATION_LABEL",
            orientation="h",
            text="TOTAL_REVENUE",
            hover_data={
                "RESTAURANT_ID": True,
                "TOTAL_ORDERS": True,
                "AVG_ORDER_VALUE": True,
                "ORDERS_PER_ACTIVE_DAY": True
            },
            labels={
                "TOTAL_REVENUE": "Total Revenue ($)",
                "LOCATION_LABEL": "Location"
            }
        )

        fig.update_traces(
            texttemplate="$%{text:,.0f}",
            textposition="outside"
        )

        fig.update_layout(
            xaxis_title="Total Revenue ($)",
            yaxis_title=None
        )

    elif selected_view == "Order Volume":

        st.subheader("Location Order Volume")

        chart_df = (
            location_df
            .sort_values(
                "TOTAL_ORDERS",
                ascending=True
            )
        )

        fig = px.bar(
            chart_df,
            x="TOTAL_ORDERS",
            y="LOCATION_LABEL",
            orientation="h",
            text="TOTAL_ORDERS",
            hover_data={
                "RESTAURANT_ID": True,
                "TOTAL_REVENUE": True,
                "AVG_ORDER_VALUE": True
            },
            labels={
                "TOTAL_ORDERS": "Total Orders",
                "LOCATION_LABEL": "Location"
            }
        )

        fig.update_traces(
            texttemplate="%{text:,.0f}",
            textposition="outside"
        )

        fig.update_layout(
            xaxis_title="Total Orders",
            yaxis_title=None
        )

    elif selected_view == "Average Order Value":

        st.subheader("Average Order Value by Location")

        chart_df = (
            location_df
            .sort_values(
                "AVG_ORDER_VALUE",
                ascending=True
            )
        )

        fig = px.bar(
            chart_df,
            x="AVG_ORDER_VALUE",
            y="LOCATION_LABEL",
            orientation="h",
            text="AVG_ORDER_VALUE",
            hover_data={
                "RESTAURANT_ID": True,
                "TOTAL_ORDERS": True,
                "TOTAL_REVENUE": True
            },
            labels={
                "AVG_ORDER_VALUE": "Average Order Value ($)",
                "LOCATION_LABEL": "Location"
            }
        )

        fig.update_traces(
            texttemplate="$%{text:.2f}",
            textposition="outside"
        )

        fig.update_layout(
            xaxis_title="Average Order Value ($)",
            yaxis_title=None
        )

    elif selected_view == "Order Activity":

        st.subheader("Orders per Active Day by Location")

        chart_df = (
            location_df
            .sort_values(
                "ORDERS_PER_ACTIVE_DAY",
                ascending=True
            )
        )

        fig = px.bar(
            chart_df,
            x="ORDERS_PER_ACTIVE_DAY",
            y="LOCATION_LABEL",
            orientation="h",
            text="ORDERS_PER_ACTIVE_DAY",
            hover_data={
                "RESTAURANT_ID": True,
                "ORDERS_PER_ACTIVE_WEEK": True,
                "ACTIVE_DAYS": True,
                "ACTIVE_WEEKS": True,
                "TOTAL_ORDERS": True
            },
            labels={
                "ORDERS_PER_ACTIVE_DAY": "Orders per Active Day",
                "LOCATION_LABEL": "Location"
            }
        )

        fig.update_traces(
            texttemplate="%{text:.2f}",
            textposition="outside"
        )

        fig.update_layout(
            xaxis_title="Orders per Active Day",
            yaxis_title=None
        )

    st.plotly_chart(
        fig,
        use_container_width=True
    )


if __name__ == "__main__":
    main()
