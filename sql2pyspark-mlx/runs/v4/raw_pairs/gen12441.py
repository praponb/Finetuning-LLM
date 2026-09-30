from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("is_new_york").desc(), "id")
w2 = Window.partitionBy("region", "city").orderBy("is_new_york")
w3 = Window.partitionBy("city").orderBy(F.col("is_new_york").desc(), "id")
w4 = Window.partitionBy("city").orderBy("is_new_york", "id")
w5 = Window.orderBy("is_new_york", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("city").orderBy(F.col("is_new_york_bucket").desc(), "id")

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.trim("region").alias("clean_region"),
        F.when(F.col("city") == "New York", 1).otherwise(0).alias("is_new_york"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

scored = (
    cleaned
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("is_new_york_cume_dist", F.cume_dist().over(w2))
    .withColumn("is_new_york_bucket", F.ntile(5).over(w3))
    .withColumn("prev_is_new_york", F.lag("is_new_york", 1).over(w4))
    .withColumn("is_new_york_avg_6_prior", F.avg("is_new_york").over(w5))
)

ranked = (
    scored
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "clean_region",
        "is_new_york",
        "event_seq",
        "is_new_york_cume_dist",
        "is_new_york_bucket",
        F.when(F.col("store_name").rlike("^(Mall Central|Siam Flagship)$"), "PRIORITY").when(F.col("store_name").rlike("^(Downtown Express|Airport Kiosk)$"), "EXTENDED").otherwise("GROUP_A").alias("store_name_zone"),
    )
    .orderBy(F.col("is_new_york_cume_dist").asc_nulls_first())
)
