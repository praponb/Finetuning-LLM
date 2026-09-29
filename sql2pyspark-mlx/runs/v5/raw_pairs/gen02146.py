from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("customer_id")
w_ordered = Window.partitionBy("customer_id", "status").orderBy("start_date", "id")

enriched = (
    df_subscriptions
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.lag("monthly_fee", 1).over(w_ordered).alias("prev_monthly_fee"),
    )
)

result = (
    enriched
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "row_seq",
        "prev_monthly_fee",
        F.when(F.col("plan").rlike("^(standard|premium)$"), "PRIORITY").otherwise("GROUP_B").alias("plan_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first(), "plan_zone")
    .limit(5)
)
