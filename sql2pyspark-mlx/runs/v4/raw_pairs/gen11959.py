from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("city").orderBy("sqft", "id")
w2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(-5, -1)

kept = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock").between("2024-11-01", "2024-12-01")), "left_anti")
)

cleaned = (
    kept
    .filter(F.year("opened_date") >= 2023)
    .withColumn("clean_store_name", F.trim("store_name"))
    .withColumn("region_filled", F.coalesce("region", F.lit("DIRECT")))
)

kept_stores = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.opened_date")), "left_anti")
)

result = (
    kept_stores
    .select(
        "*",
        F.ntile(3).over(w_part).alias("sqft_bucket"),
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
        F.avg("sqft").over(w_recent).alias("sqft_avg_2"),
        F.avg("sqft").over(w4).alias("sqft_avg_5_prior"),
    )
)
