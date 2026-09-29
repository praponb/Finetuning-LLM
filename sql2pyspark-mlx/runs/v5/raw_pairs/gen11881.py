from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")

result = (
    df_store_sales
    .select("id", "store_id", "revenue", F.row_number().over(w).alias("rn"))
    .orderBy(F.col("store_id").desc())
)
