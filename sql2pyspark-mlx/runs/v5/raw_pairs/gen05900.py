from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-04-19").cast("date")) & F.col("product_id").isin([1, 2]))
    .select("*", F.last_day("last_restock").alias("last_restock_month_end"))
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    latest
    .filter(F.col("last_restock") > "2023-08-01")
    .select(
        "*",
        F.min("quantity").over(w2).alias("running_min_quantity"),
        F.avg("quantity").over(w2).alias("running_avg_quantity"),
        F.count("quantity").over(w2).alias("running_count_quantity"),
        F.sum("quantity").over(w2).alias("running_sum_quantity"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.launch_date"), F.col("b.stock_qty"), F.col("b.category"))
)

with_dims = (
    joined
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "inner")
    .select(
        "a.id",
        "a.store_id",
        "a.quantity",
        "a.last_restock_month_end",
        "a.running_sum_quantity",
        "a.launch_date",
        F.col("s.sqft"),
    )
)

result = (
    with_dims
    .withColumn("running_sum_quantity_tier", F.when(F.col("running_sum_quantity") >= 1010, "LARGE").otherwise("SMALL"))
    .withColumn("activity_month", F.date_trunc("month", "launch_date"))
    .groupingSets(
        [["running_sum_quantity_tier", "store_id", "activity_month"], ["running_sum_quantity_tier", "store_id"], ["running_sum_quantity_tier"], []],
        "running_sum_quantity_tier",
        "store_id",
        "activity_month",
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("running_sum_quantity_tier").alias("grouping_level"),
    )
    .filter(F.col("quantity_bit_and") < 1000)
)
