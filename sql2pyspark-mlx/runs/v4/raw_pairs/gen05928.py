from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code").orderBy(F.col("num_rows").desc())

lines = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("code"))

result = (
    lines
    .groupBy("code", F.date_trunc("day", "created_at").alias("activity_day"))
    .agg(
        F.size(F.collect_set("code")).alias("num_codes"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
    )
    .withColumn("num_rows_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("activity_day").desc_nulls_first())
)
