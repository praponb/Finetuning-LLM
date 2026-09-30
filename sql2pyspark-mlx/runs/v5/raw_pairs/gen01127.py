from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id")

ranked = (
    df_products
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_rows"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.max("cost").alias("max_cost"),
        F.corr("stock_qty", "price").alias("corr_stock_qty_price"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
    )
    .orderBy(F.col("activity_month").desc())
)
