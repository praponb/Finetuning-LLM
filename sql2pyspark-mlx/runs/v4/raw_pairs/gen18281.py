from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .select("id", "status", "shipped_date", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .select("id", "status", "shipped_date")
    .orderBy("shipped_date")
)
