from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w3 = Window.orderBy(F.col("rating").desc(), "id")
w4 = Window.orderBy("product_stock_qty", "id")
w5 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_suppliers
    .filter(F.col("city").isin(["London", "Chiang Mai"]))
    .withColumn("rating_int", F.col("rating").cast("int"))
    .withColumn("adjusted_rating", F.round((F.col("rating") * 0.9), 2))
)

cleaned = (
    valid
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        (F.col("rating_int") * F.col("adjusted_rating")).alias("rating_int_times_adjusted_rating"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.city",
        "s.rating",
        "s.rating_int",
        "s.adjusted_rating",
        "s.rating_int_times_adjusted_rating",
        F.col("c.stock_qty"),
        F.col("c.cost"),
    )
)

with_last = (
    with_latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.rating",
        "s.adjusted_rating",
        "s.stock_qty",
        "s.cost",
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

with_windows = (
    with_last
    .withColumn("rating_bucket", F.ntile(10).over(w3))
    .withColumn("prev_cost", F.lag("cost", 1, 0).over(w4))
    .withColumn("first_known_adjusted_rating", F.first("adjusted_rating", ignorenulls=True).over(w5))
    .withColumn("cumulative_product_stock_qty", F.sum("product_stock_qty").over(w6))
)

eligible = (
    with_windows
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.upper(F.col("x.product_name")).like("%OGA%")), "left_semi")
)

result = (
    eligible
    .withColumn("first_known_adjusted_rating_tier", F.when(F.col("first_known_adjusted_rating") >= 84, "LARGE").otherwise("SMALL"))
    .withColumn("cost_tier", F.when(F.col("cost") >= 790, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["first_known_adjusted_rating_tier", "cost_tier"], ["first_known_adjusted_rating_tier"], []],
        "first_known_adjusted_rating_tier",
        "cost_tier",
    )
    .agg(
        F.round(F.avg("rating_bucket"), 2).alias("avg_rating_bucket"),
        F.round(F.avg("rating_bucket"), 2).alias("avg_rating_bucket_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("avg_rating_bucket_2") < 1000000)
    .orderBy("cost_tier")
)
