from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("least_units_revenue").desc_nulls_first(), "id")
w_part = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("brand").orderBy("product_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("latest_rank", "id")
w5 = Window.partitionBy("brand").orderBy("least_units_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("brand", "product_id").orderBy(F.col("price").desc())

valid = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3]))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.least("units", "revenue").alias("least_units_revenue"),
    )
)

joined = (
    valid
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.cost"), F.col("d.category"), F.col("d.price"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.category").isin(["Fashion", "Books"]))
    .select(
        "src.id",
        "src.product_id",
        "src.units",
        "src.least_units_revenue",
        "src.cost",
        "src.price",
        F.col("b.brand"),
        F.col("b.cost").alias("product_cost"),
        F.col("b.product_name"),
    )
)

ranked = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    ranked
    .withColumn("last_known_brand", F.last("brand", ignorenulls=True).over(w_part))
    .withColumn("last_known_units", F.last("units", ignorenulls=True).over(w_hist))
    .withColumn("prev_price", F.lag("price", 1, 0).over(w4))
    .withColumn("cumulative_product_cost", F.sum("product_cost").over(w5))
    .withColumn("price_rank", F.rank().over(w6))
)

result = (
    with_windows
    .cube(
        "product_id",
        F.when(F.col("price") >= 990, "HIGH").otherwise("LOW").alias("price_tier"),
        F.when(F.col("prev_price") >= 1060, "PREMIUM").otherwise("STANDARD").alias("prev_price_tier"),
    )
    .agg(
        F.bit_or("units").alias("units_bit_or"),
        F.round(F.sum("cumulative_product_cost") / F.nullif(F.count(F.when(F.col("brand") == "Globex", 1)), F.lit(0)), 2).alias("cumulative_product_cost_per_globex"),
        F.count_if(F.col("brand") == "Umbrella").alias("umbrella_events"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("brand") == "Acme", 1)), F.lit(0)), 2).alias("cost_per_acme"),
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_rows"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
    )
)
