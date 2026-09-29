from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy(F.col("monthly_fee").desc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc())

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNull() & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.when(F.col("plan").isin(["premium", "standard"]), "Core").otherwise("Other").alias("plan_group"),
    )
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "start_date",
        "plan_group",
        F.cume_dist().over(w).alias("monthly_fee_cume_dist"),
        F.cume_dist().over(w2).alias("monthly_fee_cume_dist_2"),
    )
)
