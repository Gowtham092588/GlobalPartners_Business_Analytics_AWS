import streamlit as st
import plotly.express as px
import pandas as pd
from athena import (
    get_athena_client,
    run_sql_query,
    get_results_df
)
from queries import (get_sales_trends_query, get_holiday_sales_trend_query)


@st.cache_data(ttl=3600)
def load_sales_trends_data():

    athena = get_athena_client()

    query = get_sales_trends_query()

    query_id = run_sql_query(
        athena,
        query
    )

    df = get_results_df(
        athena,
        query_id
    )

    return df


@st.cache_data(ttl=3600)
def load_holiday_sales_data():

    athena = get_athena_client()

    query = get_holiday_sales_trend_query()

    query_id = run_sql_query(
        athena,
        query
    )

    holiday_sales_df = get_results_df(
        athena,
        query_id
    )

    return holiday_sales_df


def main():

    st.set_page_config(
        page_title="Sales Trend And Seasonality",
        page_icon="📈",
        layout="wide"
    )

    if st.button("Back to Home"):
        st.switch_page("app.py")

    st.title("📈Sales Trend And Seasonality Dashboard")

    with st.sidebar:

        st.subheader("Sales Analysis")

        selected_view = st.radio(
            "Select Visualization",
            [
                "Monthly Sales Trend",
                "Top Product Categories",
                "Holiday Sales Spikes"
            ]
        )

        if st.button(
            "Refresh Data",
            use_container_width=True
        ):
            load_sales_trends_data.clear()

            # Include this once the holiday loader exists
            load_holiday_sales_data.clear()

            st.rerun()

        st.caption(
            "Reloads the latest data from Athena."
        )

    sales_trend_df = load_sales_trends_data()

    sales_trend_df["MONTH_DATE"] = pd.to_datetime(
        sales_trend_df["YEAR"].astype(str)
        + "-"
        + sales_trend_df["MONTH"].astype(str)
        + "-01"
    )

    monthly_sales_df = (
        sales_trend_df
        .groupby(
            ["YEAR", "MONTH"],
            as_index=False
        )
        .agg(
            TOTAL_SALES=("CATEGORY_SALES", "sum")
        )
    )

    category_sales_df = sales_trend_df.copy()

    category_totals_df = (
        category_sales_df
        .groupby(
            "ITEM_CATEGORY",
            as_index=False
        )
        .agg(
            TOTAL_CATEGORY_SALES=("CATEGORY_SALES", "sum")
        )
        .sort_values(
            "TOTAL_CATEGORY_SALES",
            ascending=False
        )
    )

    top_categories = (
        category_totals_df
        .head(5)["ITEM_CATEGORY"]
        .tolist()
    )

    top_category_sales_df = (
        category_sales_df[
            category_sales_df["ITEM_CATEGORY"]
            .isin(top_categories)
        ]
        .copy()
    )

    if selected_view == "Monthly Sales Trend":

        st.subheader(
            "Monthly Sales Trend"
        )

        category_fig = px.line(
            top_category_sales_df,
            x="MONTH_DATE",
            y="CATEGORY_SALES",
            color="ITEM_CATEGORY",
            markers=True,
            labels={
                "MONTH_DATE": "Month",
                "CATEGORY_SALES": "Sales ($)",
                "ITEM_CATEGORY": "Product Category"
            }
        )

        category_fig.update_layout(
            xaxis_title="Month",
            yaxis_title="Sales ($)"
        )

        st.plotly_chart(
            category_fig,
            use_container_width=True
        )

    elif selected_view == "Top Product Categories":

        st.subheader("Top Product Categories by Sales")

        category_totals_df = (
            category_sales_df
            .groupby(
                "ITEM_CATEGORY",
                as_index=False
            )
            .agg(
                TOTAL_SALES=("CATEGORY_SALES", "sum")
            )
            .sort_values(
                "TOTAL_SALES",
                ascending=False
            )
            .head(5)
        )

        fig_category = px.bar(
            category_totals_df,
            x="TOTAL_SALES",
            y="ITEM_CATEGORY",
            orientation="h",
            text="TOTAL_SALES",
            labels={
                "TOTAL_SALES": "Total Sales ($)",
                "ITEM_CATEGORY": "Product Category"
            }
        )

        fig_category.update_traces(
            texttemplate="$%{text:,.0f}",
            textposition="outside"
        )

        fig_category.update_layout(
            yaxis=dict(
                categoryorder="total ascending"
            ),
            xaxis_title="Total Sales ($)",
            yaxis_title=None,
            showlegend=False
        )

        st.plotly_chart(
            fig_category,
            use_container_width=True
        )

    elif selected_view == "Holiday Sales Spikes":

        st.subheader("Sales Trend with Holiday Spikes")

        holiday_sales_df = load_holiday_sales_data()

        if holiday_sales_df.empty:
            st.warning("No holiday sales data found.")
            return

        holiday_sales_df["DATE_KEY"] = pd.to_datetime(
            holiday_sales_df["DATE_KEY"]
        )

        holiday_df = holiday_sales_df[
            holiday_sales_df["IS_HOLIDAY"] == True
        ]

        fig = px.line(
            holiday_sales_df,
            x="DATE_KEY",
            y="DAILY_SALES",
            labels={
                "DATE_KEY": "Date",
                "DAILY_SALES": "Daily Sales ($)"
            }
        )

        fig.add_scatter(
            x=holiday_df["DATE_KEY"],
            y=holiday_df["DAILY_SALES"],
            mode="markers",
            text=holiday_df["HOLIDAY_NAME"],
            name="Holiday",
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Date: %{x}<br>"
                "Sales: $%{y:,.0f}"
                "<extra></extra>"
            )
        )

        st.plotly_chart(
            fig,
            use_container_width=True
        )


if __name__ == "__main__":
    main()
