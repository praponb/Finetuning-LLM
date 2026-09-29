from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating_filled").desc_nulls_first(), "id")
w2 = Window.partitionBy("city").orderBy("rating_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("rating_filled").desc_nulls_first())
w_ordered = Window.partitionBy("city").orderBy("rating_filled", "id").rowsBetween(-2, -1)

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("city") == "New York"))
    .withColumn("country_prefix", F.substring("country", 1, 4))
    .withColumn("rating_filled", F.coalesce("rating", F.lit(0)))
)

eligible = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.length(F.col("c.product_name")) > 5), "left_anti")
)

deduped = (
    eligible
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "supplier_name", "city", "rating_filled")
)

with_windows = (
    deduped
    .withColumn("running_rating_filled", F.sum("rating_filled").over(w2))
    .withColumn("rating_filled_cume_dist", F.cume_dist().over(w3))
    .withColumn("rating_filled_avg_2_prior", F.avg("rating_filled").over(w_ordered))
)

result = (
    with_windows
    .select(
        "id",
        "city",
        F.col("rating_filled_cume_dist").alias("RATING_FILLED_CUME_DIST"),
        "rating_filled",
    )
    .unpivot(
        ids=["id", "city"],
        values=["RATING_FILLED_CUME_DIST", "rating_filled"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
