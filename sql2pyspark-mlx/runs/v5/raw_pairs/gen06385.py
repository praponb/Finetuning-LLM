from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("rating_avg_2", "id")

valid = (
    df_suppliers
    .filter(F.col("city") == "Chiang Mai")
    .select(
        "*",
        F.when(F.col("rating") >= 4.3, "High").when(F.col("rating") >= 2.5, "Medium").otherwise("Low").alias("rating_tier"),
        F.trim("supplier_name").alias("clean_supplier_name"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "rating_tier",
        "clean_supplier_name",
    )
)

enriched = (
    latest
    .filter(F.col("country") == "SG")
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.lag("rating").over(w2).alias("prev_rating"),
        F.row_number().over(w2).alias("row_seq"),
        F.avg("rating").over(w3).alias("rating_avg_2"),
    )
)

latest_suppliers = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest_suppliers
    .withColumn("prev_rating_tier", F.when(F.col("prev_rating") >= 4.1, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["prev_rating_tier", "country"], ["prev_rating_tier"], []],
        "prev_rating_tier",
        "country",
    )
    .agg(
        F.percentile_approx("row_seq", 0.5).alias("row_seq_p50"),
        F.round(F.sum("rating_avg_2") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("rating_avg_2_per_new_york"),
        F.count_if(F.col("city") == "Singapore").alias("singapore_events"),
        F.percentile_approx("prev_rating", 0.95).alias("prev_rating_p95"),
        F.grouping("prev_rating_tier").alias("grouping_id"),
    )
    .filter(F.col("prev_rating_p95") > 0)
)
