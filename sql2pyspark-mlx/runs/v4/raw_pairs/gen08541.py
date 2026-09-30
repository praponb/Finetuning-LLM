from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 110) | F.col("plan").isin(["premium", "enterprise"]))
    .select("id", "monthly_fee", F.initcap(F.trim("status")).alias("status_title"))
    .orderBy(F.col("monthly_fee").desc())
)
