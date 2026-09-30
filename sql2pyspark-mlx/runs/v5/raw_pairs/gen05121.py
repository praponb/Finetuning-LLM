from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("k").orderBy(F.col("created_at").desc(), "id", "k")
w2 = Window.partitionBy("k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "created_at",
        "k",
        F.row_number().over(w_ordered).alias("seq_no"),
        F.first("k", ignorenulls=True).over(w2).alias("first_known_k"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)
