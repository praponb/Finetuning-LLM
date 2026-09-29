from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc())

ranked = (
    df_store_sales
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select("id", "store_id", "sale_date", "units", "revenue", "rn")
    .orderBy(F.col("rn").asc_nulls_first())
)
