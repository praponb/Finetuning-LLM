from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy("shipped_date", "id")
w2 = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_shipments
    .filter(F.col("shipped_date") > "2025-10-01")
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        F.lead("cost").over(w).alias("next_cost"),
        F.lag("cost").over(w).alias("prev_cost"),
        F.row_number().over(w).alias("row_seq"),
        F.last("cost", ignorenulls=True).over(w2).alias("last_known_cost"),
    )
)

result = (
    scored
    .select("id", "delivered_date", "cost", "next_cost", "prev_cost", "last_known_cost")
    .orderBy(F.col("next_cost").desc(), "cost")
)
