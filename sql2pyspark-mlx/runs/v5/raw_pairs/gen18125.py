from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("cost").asc_nulls_first())
w3 = Window.orderBy("delivered_date", "id")

metrics = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.last("carrier", ignorenulls=True).over(w).alias("last_known_carrier"),
        F.cume_dist().over(w2).alias("cost_cume_dist"),
        F.lead("cost", 2, 0).over(w3).alias("next_cost"),
    )
)

result = (
    metrics
    .select("id", "shipped_date", "cost", "last_known_carrier")
    .orderBy(F.col("shipped_date").desc())
)
