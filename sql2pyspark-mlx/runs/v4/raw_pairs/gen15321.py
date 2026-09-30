from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("city", "country").orderBy(F.col("rating").asc_nulls_last())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") < "2024-04-01"), "left_anti")
)

scored = (
    eligible
    .select(
        "*",
        F.lag("rating", 1).over(w).alias("prev_rating"),
        F.cume_dist().over(w2).alias("rating_cume_dist"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty"))
)

result = (
    latest_match
    .groupBy("country")
    .agg(
        F.percentile_approx("rating_cume_dist", 0.5).alias("rating_cume_dist_p50"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("stock_qty")).otherwise(0)).alias("tokyo_stock_qty"),
    )
)
