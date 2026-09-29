from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "standard", "premium"]) | F.col("plan").isin(["basic", "premium", "enterprise"]))
)
