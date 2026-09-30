from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status").orderBy("shipped_date", "id")
w2 = Window.partitionBy("status")

result = (
    df_shipments
    .filter(F.col("cost") >= 86)
    .select(
        "*",
        F.lead("cost", 1).over(w_ordered).alias("next_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)
