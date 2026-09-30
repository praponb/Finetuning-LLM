from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc(), "id")

ranked = (
    df_products
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .cube(F.when(F.col("price") >= 1010, "HIGH").otherwise("LOW").alias("price_tier"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("category") == "Home", F.col("cost")).otherwise(0)).alias("home_cost"),
    )
    .orderBy(F.col("cnt").asc_nulls_first())
)
