from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")

scored = (
    df_shipments
    .select(
        "*",
        F.sum("cost").over(w).alias("cumulative_cost"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

result = scored.select("id", "carrier", "shipped_date", "delivered_date", "cost", "status")
