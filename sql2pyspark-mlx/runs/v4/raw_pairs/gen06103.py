from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc(), "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")
w5 = Window.partitionBy("store_id").orderBy("revenue", "id")

enriched = (
    df_store_sales
    .withColumn("cumulative_revenue", F.sum("revenue").over(w))
    .withColumn("first_known_revenue", F.first("revenue", ignorenulls=True).over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("next_revenue", F.lead("revenue", 1, 0).over(w5))
)

result = (
    enriched
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        "first_known_revenue",
        "next_revenue",
    )
    .orderBy(F.col("next_revenue").desc_nulls_last())
)
