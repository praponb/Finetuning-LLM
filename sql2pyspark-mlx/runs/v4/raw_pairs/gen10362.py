from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("units", "id")
w_ordered = Window.partitionBy("city").orderBy("price", "id")
w4 = Window.partitionBy("city").orderBy(F.col("units").desc(), "id")
w5 = Window.partitionBy("city").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id")
w8 = Window.partitionBy("store_id").orderBy("sale_date", "id")

with_dims = (
    df_store_sales
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.price"))
)

base = (
    with_dims
    .filter((F.col("sale_date") >= F.lit("2023-05-23").cast("date")) & (F.year("sale_date") == 2023))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "price",
        F.date_sub("sale_date", 36).alias("sale_date_minus_36d"),
    )
)

with_dims_store_sales = (
    base
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.city"))
)

enriched = (
    with_dims_store_sales
    .select(
        "*",
        F.last("price", ignorenulls=True).over(w).alias("last_known_price"),
        F.row_number().over(w2).alias("event_seq"),
        F.lead("units", 1).over(w_ordered).alias("next_units"),
        F.ntile(3).over(w4).alias("units_bucket"),
        F.last("units", ignorenulls=True).over(w5).alias("last_known_units"),
    )
)

enriched_store_sales = (
    enriched
    .withColumn("first_known_price", F.first("price", ignorenulls=True).over(w6))
    .withColumn("last_known_units_share", F.round((F.col("last_known_units") / F.sum("last_known_units").over(w7)), 4))
    .withColumn("next_next_units", F.lead("next_units", 1).over(w8))
)

result = (
    enriched_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date_minus_36d").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
        F.count("*").alias("num_rows"),
        F.count_if(F.col("city") == "Berlin").alias("berlin_events"),
        F.round(F.sum("last_known_price") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("last_known_price_per_new_york"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("last_known_units_share"))).alias("bangkok_total_last_known_units_share"),
    )
    .orderBy(F.col("bangkok_total_last_known_units_share").asc_nulls_last())
)
