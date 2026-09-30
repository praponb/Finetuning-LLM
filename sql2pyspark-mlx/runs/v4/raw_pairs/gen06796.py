from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy(F.col("start_date_month_end").desc_nulls_first(), "id")

filtered = (
    df_subscriptions
    .filter(F.col("start_date") >= F.lit("2024-04-03").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.last_day("start_date").alias("start_date_month_end"),
        F.date_add("end_date", 30).alias("end_date_plus_30d"),
    )
)

result = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
