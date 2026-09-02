import pandas as pd
import os
import sys

from streamlit_app.metrics import calculate_churn_kpis

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def test_calculate_churn_kpis():

    test_df = pd.DataFrame({
        "USER_ID": [
            "U1",
            "U2",
            "U3",
            "U4"
        ],

        "CHURN_STATUS": [
            "AT_RISK",
            "ACTIVE",
            "AT_RISK",
            "ACTIVE"
        ],

        "DAYS_SINCE_LAST_ORDER": [
            100,
            20,
            80,
            10
        ],

        "SPEND_CHANGE_PCT": [
            -20,
            10,
            -5,
            15
        ]
    })

    result = calculate_churn_kpis(test_df)

    assert result["at_risk_customers"] == 2

    assert result["churn_risk_rate"] == 50.0

    assert result["avg_days_since_last_order"] == 52.5

    assert result["declining_spend_customers"] == 2
