from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 160) & F.col("plan").isin(["premium", "basic"]))
    .orderBy(F.col("id").desc())
)
