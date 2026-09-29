from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost_filled").asc_nulls_first(), "id")

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("status") == "RETURNED"))
    .withColumn("cost_adjusted", F.round((F.col("cost") * 0.85), 2))
    .withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
)

result = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_id", "carrier", "shipped_date")
)
