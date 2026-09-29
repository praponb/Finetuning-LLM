from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("created_at").desc(), "id", "k")
w2 = Window.orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("attributes").alias("k", "v"))
)

result = (
    lines
    .select(
        "*",
        F.row_number().over(w_running).alias("event_seq"),
        F.first("k", ignorenulls=True).over(w2).alias("first_known_k"),
    )
)
