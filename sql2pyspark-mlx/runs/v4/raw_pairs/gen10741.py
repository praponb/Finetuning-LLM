from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .select("id", "supplier_id", "launch_date", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "supplier_id", "launch_date")
    .orderBy(F.col("id").desc())
)
