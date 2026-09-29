from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("is_th", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("is_th", "id")
w4 = Window.partitionBy("city").orderBy(F.col("running_rating").desc(), "id")
w5 = Window.partitionBy("city")

base = (
    df_suppliers
    .filter(F.col("rating").between(2.7, 3.8))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.when(F.col("country") == "TH", 1).otherwise(0).alias("is_th"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

scored = (
    base
    .withColumn("first_known_is_th", F.first("is_th", ignorenulls=True).over(w))
    .withColumn("running_rating", F.sum("rating").over(w_recent))
    .withColumn("prev_rating", F.lag("rating", 1).over(w3))
)

with_windows = (
    scored
    .withColumn("running_rating_bucket", F.ntile(10).over(w4))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w5)), 4))
)

grouped = (
    with_windows
    .groupBy(
        "city",
        F.when(F.col("first_known_is_th") >= 69, "HIGH").otherwise("LOW").alias("first_known_is_th_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("rating")).otherwise(0)).alias("chiang_mai_rating"),
    )
    .filter(F.col("event_count") < 1)
)

result = (
    grouped
    .select(
        "city",
        "first_known_is_th_tier",
        "event_count",
        F.when(F.col("city").rlike("^(Tokyo|Chiang Mai)$"), "GROUP_A").otherwise("CORE").alias("city_zone"),
    )
)
