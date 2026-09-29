from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").asc_nulls_first())

deduped = (
    df_store_sales
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = deduped.select("id", "store_id", "product_id", "units", "dedup_rank")
