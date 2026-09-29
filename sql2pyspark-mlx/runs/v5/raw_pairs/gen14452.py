from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc(), "id")

ranked = (
    df_store_sales
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "product_id", "sale_date")
)

result = (
    ranked
    .select("id", "sale_date")
    .orderBy(F.col("sale_date").asc_nulls_first(), "id")
    .limit(20)
)
