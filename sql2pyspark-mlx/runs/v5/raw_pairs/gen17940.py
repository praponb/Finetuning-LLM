from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("status")

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("status") != "ACTIVE"))
    .select("*", F.col("monthly_fee").cast("int").alias("monthly_fee_int"))
)

result = (
    valid
    .select(
        "id",
        "plan",
        "monthly_fee",
        "status",
        "monthly_fee_int",
        F.lag("monthly_fee").over(w).alias("prev_monthly_fee"),
        F.lead("monthly_fee").over(w).alias("next_monthly_fee"),
        F.lead("monthly_fee_int").over(w).alias("next_monthly_fee_int"),
        F.row_number().over(w).alias("event_seq"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
    )
)
