from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("country").orderBy("rating", "id")
w_hist = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-3, -1)
w5 = Window.orderBy("rating", "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("product_brand").orderBy(F.col("row_seq").desc(), "id")
w8 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("product_brand", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("country").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("country", "product_brand").orderBy("row_seq_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

not_flagged = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.price") >= 340), "left_anti")
)

scored = (
    not_flagged
    .withColumn("cumulative_rating", F.sum("rating").over(w_grp))
    .withColumn("rating_bucket", F.ntile(10).over(w_part))
    .withColumn("rating_avg_6_prior", F.avg("rating").over(w_hist))
    .withColumn("rating_avg_3_prior", F.avg("rating").over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
)

latest_match = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.rating",
        "s.row_seq",
        F.col("c.brand").alias("product_brand"),
    )
)

filtered = (
    latest_match
    .filter(F.col("product_brand").isNotNull() & (~F.col("product_brand").eqNullSafe(F.lit("Umbrella"))))
    .select(
        "*",
        F.concat_ws("/", "product_brand", "country").alias("product_brand_country_key"),
        F.coalesce("row_seq", F.lit(0)).alias("row_seq_filled"),
    )
)

with_windows = (
    filtered
    .withColumn("row_seq_bucket", F.ntile(10).over(w7))
    .withColumn("last_known_row_seq_filled", F.last("row_seq_filled", ignorenulls=True).over(w8))
    .withColumn("cumulative_rating", F.sum("rating").over(w9))
    .withColumn("first_known_rating", F.first("rating", ignorenulls=True).over(w10))
    .withColumn("last_known_row_seq_filled_2", F.last("row_seq_filled", ignorenulls=True).over(w_running))
)

result = (
    with_windows
    .select(
        "id",
        "country",
        "rating",
        "row_seq",
        "product_brand",
        "last_known_row_seq_filled",
        "cumulative_rating",
        F.when(F.col("product_brand").rlike("^(Initech|Umbrella)$"), "EXTENDED").when(F.col("product_brand").rlike("^(Acme|Globex)$"), "CORE").otherwise("PRIORITY").alias("product_brand_zone"),
    )
    .orderBy(F.col("product_brand").desc(), "row_seq")
)
