from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 80) | (F.col("plan") == "standard"))
    .select("plan", F.initcap(F.trim("plan")).alias("plan_title"))
)
