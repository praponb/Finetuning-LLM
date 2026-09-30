from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("rating").desc())
w4 = Window.orderBy("rating", "id")
w5 = Window.partitionBy("city", "country").orderBy("country_avg_rating", "id")
w6 = Window.partitionBy("city", "country").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city", "country").orderBy("rating_rank", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.partitionBy("country").orderBy(F.col("seq_no").desc(), "id")
w9 = Window.partitionBy("country").orderBy(F.col("cumulative_country_count_rating").desc(), "id")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.concat_ws("-", "city", "country").alias("city_country_key"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.avg("rating").over(w).alias("country_avg_rating"),
        F.first("city", ignorenulls=True).over(w2).alias("first_known_city"),
        F.rank().over(w3).alias("rating_rank"),
        F.row_number().over(w4).alias("seq_no"),
        F.count("rating").over(w).alias("country_count_rating"),
    )
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "first_known_city",
        "rating_rank",
        "seq_no",
        "country_count_rating",
    )
)

enriched = (
    latest
    .filter(F.col("city").isin(["London", "Singapore", "Berlin"]))
    .select(
        "*",
        F.sum("country_count_rating").over(w6).alias("cumulative_country_count_rating"),
        F.avg("seq_no").over(w7).alias("seq_no_avg_6"),
        F.ntile(5).over(w8).alias("seq_no_bucket"),
    )
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select("id", "country", "first_known_city", "rating_rank", "seq_no")
)

result = (
    deduped
    .select("id", "country", "rating_rank", "seq_no")
    .orderBy(F.col("country").asc_nulls_first())
)
