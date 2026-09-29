from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2024-05-01", "2025-05-01"))
    .select(
        "id",
        "carrier",
        "cost",
        "delivered_date",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 3)
    .select("id", "carrier", "cost", "delivered_date")
    .orderBy(F.col("delivered_date").desc(), "carrier")
)
