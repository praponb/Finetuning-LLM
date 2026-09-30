from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city", "country").orderBy("product_launch_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("country", "city").orderBy("product_cost", "id")
w4 = Window.partitionBy("country", "city").orderBy("product_launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy(F.col("product_launch_date").desc())

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.launch_date") >= "2023-12-15"), "left_semi")
)

with_last = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.country",
        "s.city",
        "s.rating",
        F.col("c.cost").alias("product_cost"),
        F.col("c.launch_date").alias("product_launch_date"),
    )
)

with_windows = (
    with_last
    .select(
        "*",
        F.avg("product_cost").over(w2).alias("product_cost_avg_3"),
        F.row_number().over(w3).alias("row_seq"),
        F.sum("rating").over(w4).alias("running_rating"),
    )
)

latest = (
    with_windows
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "product_cost",
        "product_launch_date",
    )
)

stats = (
    latest
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "TH", F.col("product_cost")).otherwise(0)).alias("th_product_cost"),
        F.count("*").alias("row_count"),
        F.count("*").alias("event_count"),
    )
)

result = (
    stats
    .select("country", "th_product_cost", "row_count")
    .orderBy("th_product_cost", "country")
)
