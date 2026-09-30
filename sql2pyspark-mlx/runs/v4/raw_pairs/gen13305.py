from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .filter(F.col("carrier") == "DHL")
    .select(
        "id",
        "carrier",
        "shipped_date",
        F.row_number().over(w).alias("shipped_date_row"),
    )
    .orderBy(F.col("id").desc(), F.col("shipped_date_row").desc())
)
