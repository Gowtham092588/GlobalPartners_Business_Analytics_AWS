import streamlit as st


import streamlit as st


def main():

    st.set_page_config(
        page_title="Restaurant Customer Analytics Platform",
        page_icon="📊",
        layout="wide"
    )

    # -----------------------------------------
    # Main page header
    # -----------------------------------------

    st.title("📊 Restaurant Customer Analytics Platform")

    with st.container(border=True):

        st.markdown("### Platform Summary")

        st.write(
            "This analytics platform provides a unified view of customer behavior "
            "and restaurant performance. It combines customer segmentation, churn "
            "indicators, sales trends, loyalty analysis, and location performance "
            "to support marketing, retention, staffing, and operational decisions."
        )

    st.write(
        "Select a dashboard below to explore the analysis you are interested in."
    )

    st.divider()

    # -----------------------------------------
    # Dashboard configuration
    # -----------------------------------------

    dashboard_pages = {

        "Customer Segmentation":
            "pages/customer_segmentation_page.py",

        "Churn Risk Indicators":
            "pages/churn_risk_indicator_page.py",

        "Sales Trends & Seasonality":
            "pages/sales_trends_seasonality_page.py",

        "Loyalty Program Impact":
            "pages/loyalty_program_impact_page.py",

        "Location Performance":
            "pages/location_performance_page.py"
    }

    # -----------------------------------------
    # Dashboard selection
    # -----------------------------------------

    st.subheader("Choose a Dashboard")

    selected_dashboard = st.radio(
        "Dashboard",
        list(dashboard_pages.keys()),
        label_visibility="collapsed"
    )

    st.write("")

    if st.button(
        "Open Dashboard",
        type="primary"
    ):
        st.switch_page(
            dashboard_pages[selected_dashboard]
        )


if __name__ == "__main__":
    main()
