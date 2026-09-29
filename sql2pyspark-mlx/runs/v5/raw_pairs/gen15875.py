from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").desc_nulls_last(), "id")

valid = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-05-18").cast("date")) & F.col("customer_id").isin([4, 7, 17]))
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        F.upper("plan").alias("plan_upper"),
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)

result = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)
