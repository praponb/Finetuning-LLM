from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_first())

ranked = (
    df_store_sales
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .select("id", "product_id", "sale_date", "units", "revenue")
    .orderBy(F.col("product_id").asc_nulls_last(), F.col("units").desc())
)
