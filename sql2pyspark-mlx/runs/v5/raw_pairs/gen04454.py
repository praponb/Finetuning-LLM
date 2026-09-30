from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id")
w2 = Window.orderBy(F.col("rating").desc(), "id")
w3 = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("rating_bucket_2").desc_nulls_last())
w5 = Window.partitionBy("city").orderBy("rating_bucket_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("city").orderBy(F.col("running_min_first_known_rating").desc(), "id")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("country").isin(["US", "SG"]))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.when(F.col("rating") >= 4.5, "High").when(F.col("rating") >= 1.9, "Medium").otherwise("Low").alias("rating_band"),
    )
)

enriched = (
    cleaned
    .filter(~F.col("city").isin(["Berlin", "London", "Chiang Mai"]))
    .withColumn("rating_bucket", F.ntile(10).over(w))
    .withColumn("rating_bucket_2", F.ntile(10).over(w2))
    .withColumn("first_known_rating", F.first("rating", ignorenulls=True).over(w3))
)

eligible = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (~F.col("c.brand").isin(["Acme", "Initech", "Stark"])), "left_anti")
)

deduped = (
    eligible
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select("id", "city", "rating", "rating_bucket_2", "first_known_rating")
)

scored = (
    deduped
    .select(
        "id",
        "city",
        "rating",
        "rating_bucket_2",
        F.avg("first_known_rating").over(w5).alias("running_avg_first_known_rating"),
        F.min("first_known_rating").over(w5).alias("running_min_first_known_rating"),
        F.sum("first_known_rating").over(w5).alias("running_sum_first_known_rating"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"), F.col("c.stock_qty"))
)

latest = (
    with_latest
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
    .select("id", "city", "stock_qty")
)

result = latest.select("id")
