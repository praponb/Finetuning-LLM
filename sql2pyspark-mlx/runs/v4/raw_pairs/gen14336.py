from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id", "plan").orderBy(F.col("end_date_plus_months").desc_nulls_first(), "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc(), "id")
w4 = Window.partitionBy("customer_id").orderBy("rn", "id")

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.add_months("end_date", 12).alias("end_date_plus_months"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

deduped_subscriptions = (
    deduped
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped_subscriptions
    .select(
        "*",
        F.row_number().over(w3).alias("row_seq"),
        F.lag("monthly_fee", 2).over(w4).alias("prev_monthly_fee"),
    )
)

result = (
    enriched
    .select("id", "plan", "prev_monthly_fee", F.col("monthly_fee").alias("MONTHLY_FEE"))
    .unpivot(
        ["id", "plan"],
        ["prev_monthly_fee", "MONTHLY_FEE"],
        "metric_name",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
