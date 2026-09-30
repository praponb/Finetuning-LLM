from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc(), "id")
w2 = Window.partitionBy("category").orderBy("margin", "id")
w3 = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("brand").orderBy(F.col("cost").desc(), "id")
w5 = Window.partitionBy("category").orderBy(F.col("first_known_stock_qty").desc(), "id")

cleaned = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("days_since_launch_date", F.datediff(F.current_date(), "launch_date"))
    .withColumn("margin", (F.col("price") + F.col("cost")))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("OTHER")))
)

metrics = (
    cleaned
    .withColumn("stock_qty_bucket", F.ntile(4).over(w))
    .withColumn("next_days_since_launch_date", F.lead("days_since_launch_date", 1).over(w2))
    .withColumn("first_known_stock_qty", F.first("stock_qty", ignorenulls=True).over(w3))
    .withColumn("row_seq", F.row_number().over(w_hist))
)

kept = (
    metrics
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.product_id").isNotNull()), "left_anti")
)

ranked = (
    kept
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select(
        "id",
        "category",
        "brand",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "days_since_launch_date",
        "margin",
        "resolved_brand",
        F.when(F.col("brand").rlike("^(Stark|Umbrella)$"), "GROUP_A").when(F.col("brand").rlike("^(Acme|Globex)$"), "CORE").otherwise("EXTENDED").alias("brand_zone"),
    )
)
