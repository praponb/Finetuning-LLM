from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("rating").desc_nulls_last())
w4 = Window.partitionBy("city", "country")
w5 = Window.partitionBy("category").orderBy("running_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country")

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"), F.col("c.brand").alias("product_brand"))
)

with_windows = (
    with_last
    .select(
        "*",
        F.sum("rating").over(w2).alias("running_rating"),
        F.percent_rank().over(w3).alias("rating_percent_rank"),
        F.round((F.col("rating") / F.sum("rating").over(w4)), 4).alias("rating_share"),
    )
)

scored = (
    with_windows
    .withColumn("first_known_rating_percent_rank", F.first("rating_percent_rank", ignorenulls=True).over(w5))
    .withColumn("running_rating_share", F.round((F.col("running_rating") / F.sum("running_rating").over(w6)), 4))
)

result = (
    scored
    .groupingSets(
        [["product_brand", "city"], ["product_brand"]],
        "product_brand",
        "city",
    )
    .agg(
        F.count_if(F.col("category") == "Home").alias("home_events"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("rating")).otherwise(0)).alias("tokyo_rating"),
        F.percentile_approx("rating", 0.5).alias("rating_p50"),
    )
)
