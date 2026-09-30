from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.orderBy("monthly_fee", "id")

scored = (
    df_subscriptions
    .select(
        "*",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.ntile(10).over(w2).alias("monthly_fee_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        "monthly_fee_bucket",
        F.when(F.col("plan").rlike("^(enterprise|basic)$"), "EXTENDED").otherwise("GROUP_A").alias("plan_zone"),
    )
    .orderBy("customer_id")
)
