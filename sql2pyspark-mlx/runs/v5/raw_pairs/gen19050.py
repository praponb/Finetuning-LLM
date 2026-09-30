from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date")

result = (
    df_products
    .select("id", "supplier_id", "launch_date", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "supplier_id", "launch_date")
    .orderBy("launch_date")
)
