from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("attr_key").orderBy("created_at", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("customer_id").orderBy("created_at", "id", "attr_key")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

result = (
    flattened
    .select(
        "id",
        "attr_key",
        "v",
        F.first("attr_key", ignorenulls=True).over(w_part).alias("first_known_attr_key"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)
