from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")

result = (
    df_shipments
    .filter(F.col("cost").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .select("id", "carrier", "shipped_date", "cost")
)
