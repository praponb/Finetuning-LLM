from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .select(
        "id",
        "status",
        "shipped_date",
        "carrier",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 2)
    .select("id", "status", "shipped_date", "carrier")
    .orderBy(F.col("status").desc(), "carrier")
)
