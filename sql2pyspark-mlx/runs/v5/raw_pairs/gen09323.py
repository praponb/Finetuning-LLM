from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")
w2 = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["DHL", "UPS", "Kerry"]))
    .select(
        "id",
        "order_id",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.sum("cost").over(w2).alias("cumulative_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share_2"),
        F.ntile(5).over(w3).alias("cost_bucket"),
    )
)
