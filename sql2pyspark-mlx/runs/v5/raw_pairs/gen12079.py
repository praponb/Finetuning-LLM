from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("activity_day").orderBy(F.col("duration_min").desc())

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

result = (
    exploded
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"), "attr_key")
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("attr_key") == "gift", 1)).alias("gift_count"),
    )
    .withColumn("duration_min_rank", F.rank().over(w))
    .orderBy(F.col("attr_key").desc())
)
