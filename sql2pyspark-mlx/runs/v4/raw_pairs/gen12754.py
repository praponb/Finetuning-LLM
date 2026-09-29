from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id")

result = (
    df_shipments
    .filter(F.col("carrier") == "Kerry")
    .select("id", "status", "shipped_date", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .orderBy(F.col("shipped_date").desc())
)
