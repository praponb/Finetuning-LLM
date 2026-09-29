from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit", "id")
w2 = Window.partitionBy("segment").orderBy("age", "id")
w3 = Window.partitionBy("country")
w4 = Window.orderBy(F.col("credit_limit").asc_nulls_last())
w5 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc_nulls_last())
w6 = Window.partitionBy("city").orderBy(F.col("age").desc(), "id")

base = (
    df_customers
    .filter(F.col("segment").isNotNull() & (F.col("country") != "SG"))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "credit_limit",
        "age",
        F.upper("segment").alias("segment_upper"),
        F.coalesce("segment", F.lit("DIRECT")).alias("segment_clean"),
    )
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    deduped
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w3)), 4))
    .withColumn("credit_limit_cume_dist", F.cume_dist().over(w4))
    .withColumn("credit_limit_cume_dist_2", F.cume_dist().over(w5))
    .withColumn("age_bucket", F.ntile(3).over(w6))
)

result = (
    with_windows
    .groupBy("city")
    .pivot("country", ["TH", "DE"])
    .agg(F.sum("age").alias("amount"))
    .select("city", F.col("TH").alias("th"), F.col("DE").alias("de"))
)
