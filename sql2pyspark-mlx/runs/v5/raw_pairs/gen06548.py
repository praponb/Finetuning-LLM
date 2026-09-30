from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_first())

base = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "standard", "basic"]))
    .select("*", F.concat("plan", F.lit(" "), "status").alias("plan_status"))
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        F.lag("monthly_fee").over(w).alias("prev_monthly_fee"),
        F.lead("monthly_fee").over(w).alias("next_monthly_fee"),
        F.row_number().over(w).alias("seq_no"),
        F.dense_rank().over(w2).alias("monthly_fee_dense_rank"),
    )
)
