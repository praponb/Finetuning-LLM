from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category").orderBy(F.col("stock_qty").desc_nulls_last(), "id")

deduped = (
    df_products
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "product_name", "brand", "price", "cost", "launch_date", "tags")
)

result = (
    deduped
    .groupBy(
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
        F.date_trunc("YEAR", "launch_date").alias("year_start"),
    )
    .agg(
        F.max("price").alias("max_price"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("price")).otherwise(0)).alias("initech_price"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.percentile_approx("price", 0.95).alias("price_p95"),
    )
    .orderBy(F.col("week_start").desc_nulls_last())
)
