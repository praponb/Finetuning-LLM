from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w3 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_first(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("stock_qty", "id")
w7 = Window.partitionBy("city").orderBy("rn", "id")
w8 = Window.partitionBy("country").orderBy("launch_date", "id")
w9 = Window.partitionBy("city").orderBy("rating", "id")

ranked = (
    df_suppliers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

cleaned = (
    deduped
    .filter(F.col("rn") == 1)
    .select(
        "*",
        F.concat("supplier_name", F.lit(" - "), "country").alias("supplier_name_country"),
        F.concat("country", F.lit(" "), "city").alias("country_city"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

latest = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest_suppliers = (
    latest
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

latest_match = (
    latest_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w4))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.launch_date"), F.col("c.stock_qty"))
)

with_windows = (
    latest_match
    .withColumn("cumulative_rating", F.sum("rating").over(w5))
    .withColumn("next_rating", F.lead("rating", 1).over(w6))
    .withColumn("event_seq", F.row_number().over(w7))
    .withColumn("prev_stock_qty", F.lag("stock_qty", 1).over(w8))
    .withColumn("rating_bucket", F.ntile(3).over(w9))
)

result = (
    with_windows
    .groupBy("country")
    .pivot("city", ["Singapore", "New York"])
    .agg(F.avg("stock_qty").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "country",
        F.col("Singapore_value_sum").alias("singapore_value_sum"),
        F.col("Singapore_item_count").alias("singapore_item_count"),
        F.col("New York_value_sum").alias("new_york_value_sum"),
        F.col("New York_item_count").alias("new_york_item_count"),
    )
)
