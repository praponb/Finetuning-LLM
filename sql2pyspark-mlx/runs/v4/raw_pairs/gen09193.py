from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("k").orderBy(F.col("created_at").desc(), "id", "k")
w3 = Window.partitionBy("k").orderBy("created_at", "id", "k")

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "value"),
    )
)

result = (
    exploded
    .withColumn("first_known_k", F.first("k", ignorenulls=True).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("last_known_k", F.last("k", ignorenulls=True).over(w))
)
