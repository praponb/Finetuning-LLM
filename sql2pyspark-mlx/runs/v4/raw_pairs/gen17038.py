from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("category").orderBy(F.col("last_known_stock_qty").desc(), "id")
w4 = Window.partitionBy("category").orderBy("last_known_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select("*", F.upper("brand").alias("brand_upper"))
)

with_windows = (
    cleaned
    .withColumn("first_known_brand", F.first("brand", ignorenulls=True).over(w))
    .withColumn("last_known_stock_qty", F.last("stock_qty", ignorenulls=True).over(w2))
)

metrics = (
    with_windows
    .withColumn("last_known_stock_qty_bucket", F.ntile(3).over(w_hist))
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w4))
)

with_dims = (
    metrics
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.country"), F.col("d.city"))
)

filtered = (
    with_dims
    .filter((F.col("launch_date") >= F.lit("2024-03-22").cast("date")) & F.col("city").isNotNull() & (F.col("price") < 1090))
    .withColumn("clean_country", F.trim("country"))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("NONE")))
)

joined = (
    filtered
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "inner")
    .select(
        "e.*",
        F.col("p.rating"),
        F.col("p.city").alias("supplier_city"),
        F.col("p.country").alias("supplier_country"),
    )
)

result = (
    joined
    .cube(
        F.when(F.col("last_known_stock_qty_bucket") >= 8, "LARGE").otherwise("SMALL").alias("last_known_stock_qty_bucket_tier"),
        F.date_trunc("week", "launch_date").alias("week_start"),
    )
    .agg(
        F.percentile_approx("rating", 0.5).alias("rating_p50"),
        F.count(F.when(F.col("supplier_country") == "DE", 1)).alias("de_rows"),
    )
)
