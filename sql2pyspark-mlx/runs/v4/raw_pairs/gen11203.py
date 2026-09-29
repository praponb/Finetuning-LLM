from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("created_at").desc(), "id", "attr_key")
w2 = Window.orderBy("created_at", "id", "attr_key")
w3 = Window.partitionBy("customer_id").orderBy("created_at", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.first("attr_key", ignorenulls=True).over(w3).alias("first_known_attr_key"),
    )
)
