from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 2, 4, 5]))
    .select("id", "store_id", "sale_date", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .select("id", "store_id", "sale_date")
)
