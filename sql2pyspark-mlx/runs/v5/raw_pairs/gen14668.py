from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category").orderBy("stock_qty", "id")
w3 = Window.orderBy("stock_qty", "id")
w4 = Window.partitionBy("brand").orderBy(F.col("running_cost").desc_nulls_first(), "id")
w5 = Window.partitionBy("brand")
w6 = Window.orderBy("rn", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-08-24").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.last_day("launch_date").alias("launch_date_month_end"),
    )
)

enriched = (
    base
    .withColumn("running_cost", F.sum("cost").over(w))
    .withColumn("stock_qty_bucket", F.ntile(10).over(w2))
    .withColumn("prev_cost", F.lag("cost", 1).over(w3))
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

scored = (
    deduped
    .filter(F.col("cost") <= 520)
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "stock_qty",
        "stock_qty_bucket",
        "prev_cost",
        F.sum("cost").over(w5).alias("brand_sum_cost"),
        F.avg("running_cost").over(w6).alias("running_cost_avg_6"),
    )
)

cleaned = (
    scored
    .filter(F.col("cost") <= 300)
    .select(
        "id",
        "category",
        "cost",
        "stock_qty",
        "stock_qty_bucket",
        "prev_cost",
        "brand_sum_cost",
        "running_cost_avg_6",
        F.when(F.col("stock_qty") >= 435, "High").when(F.col("stock_qty") >= 77, "Medium").otherwise("Low").alias("stock_qty_level"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "category",
        "stock_qty",
        "prev_cost",
        "brand_sum_cost",
        "running_cost_avg_6",
        "stock_qty_level",
        F.when(F.col("category").rlike("^(Fashion|Electronics)$"), "GROUP_B").when(F.col("category").rlike("^(Home|Sports)$"), "GROUP_A").otherwise("PRIORITY").alias("category_zone"),
    )
    .orderBy(F.col("running_cost_avg_6").desc_nulls_last(), "category_zone")
)
