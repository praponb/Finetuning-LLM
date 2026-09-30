from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("region").orderBy("last_restock", "id")
w_recent = Window.orderBy(F.col("sqft").desc(), "id")
w5 = Window.partitionBy("city").orderBy("sqft", "id")
w6 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

with_latest = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"))
)

enriched = (
    with_latest
    .withColumn("sqft_avg_6", F.avg("sqft").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("sqft_bucket", F.ntile(5).over(w_recent))
    .withColumn("prev_sqft", F.lag("sqft", 2).over(w5))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w6))
)

with_last = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"))
)

result = (
    with_last
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        "last_restock",
        "sqft_avg_6",
        "seq_no",
        "sqft_bucket",
    )
)
