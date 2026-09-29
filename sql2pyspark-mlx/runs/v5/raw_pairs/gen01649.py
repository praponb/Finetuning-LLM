from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy("cost", "id")
w2 = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_products
    .withColumn("next_stock_qty", F.lead("stock_qty").over(w))
    .withColumn("prev_stock_qty", F.lag("stock_qty").over(w))
    .withColumn("next_cost", F.lead("cost").over(w))
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("running_stock_qty", F.sum("stock_qty").over(w2))
)

result = (
    scored
    .groupBy(F.date_trunc("YEAR", "launch_date").alias("year_start"), "supplier_id")
    .agg(
        F.countDistinct("category").alias("distinct_categories"),
        F.min("price").alias("min_price"),
        F.countDistinct("category").alias("distinct_categories_2"),
    )
    .orderBy(F.col("distinct_categories").desc_nulls_first())
)
