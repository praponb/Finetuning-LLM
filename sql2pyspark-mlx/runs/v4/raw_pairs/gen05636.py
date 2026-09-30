from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("order_id", "product_id")
w_ordered = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")
w_grp = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_order_items
    .select(
        "*",
        F.corr("quantity", "unit_price").over(w_hist).alias("corr_quantity_unit_price"),
        F.lead("quantity", 2).over(w_ordered).alias("next_quantity"),
        F.first("quantity", ignorenulls=True).over(w_grp).alias("first_known_quantity"),
    )
)

result = (
    with_windows
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 8, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["quantity_tier", "order_id"], ["quantity_tier"], []],
        "quantity_tier",
        "order_id",
    )
    .agg(
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.percentile_approx("next_quantity", [0.25, 0.5, 0.75]).alias("next_quantity_quartiles"),
        F.grouping("quantity_tier").alias("gid"),
    )
    .filter(F.col("unit_price_stddev") > 0)
    .orderBy(F.col("order_id").desc())
)
