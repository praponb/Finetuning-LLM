from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("status", "plan")
    .subtract(
        df_subscriptions
        .filter(F.year("end_date") == 2025)
        .select("status", "plan")
    )
)
