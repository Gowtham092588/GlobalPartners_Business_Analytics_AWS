def calculate_churn_kpis(churn_df):

    # 1. count AT_RISK customers
    at_risk_customers = churn_df[churn_df["CHURN_STATUS"]
                                 == "AT_RISK"]["USER_ID"].nunique()

    # 2. calculate total customers
    total_customers = churn_df["USER_ID"].nunique()

    # 3. calculate churn risk percentage
    if total_customers > 0:
        churn_risk_rate = (
            at_risk_customers
            / total_customers
        ) * 100
    else:
        churn_risk_rate = 0

    # 4. calculate average DAYS_SINCE_LAST_ORDER
    avg_days_since_last_order = churn_df["DAYS_SINCE_LAST_ORDER"].mean()

    # 5. count customers where SPEND_CHANGE_PCT < 0
    declining_spend_customers = churn_df[churn_df["SPEND_CHANGE_PCT"] < 0]["USER_ID"].nunique(
    )

    return {
        "at_risk_customers": at_risk_customers,
        "churn_risk_rate": churn_risk_rate,
        "avg_days_since_last_order": avg_days_since_last_order,
        "declining_spend_customers": declining_spend_customers
    }
