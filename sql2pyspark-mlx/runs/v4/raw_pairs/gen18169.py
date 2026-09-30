from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") < 220) & ((F.col("plan") != "standard") | F.col("monthly_fee").isNotNull()))
    .select("id", "end_date", "status")
)
