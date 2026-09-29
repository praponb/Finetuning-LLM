from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_cost").desc())

result = (
    df_products
    .filter(F.col("product_name").like("%Cancel%"))
    .groupBy("supplier_id")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.rank().over(w))
    .orderBy(F.col("supplier_id").desc())
    .limit(1)
)
