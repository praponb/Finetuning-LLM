from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("is_cart", "id")
w2 = Window.orderBy("is_cart", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page", "session_id").orderBy(F.col("event_time").desc(), "id")
w4 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("event_type").orderBy("is_cart", "id")

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-10-24 05:00:00").cast("timestamp"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.when(F.col("page") == "/cart", 1).otherwise(0).alias("is_cart"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "session_id",
        "event_time",
        F.lag("is_cart", 1).over(w).alias("prev_is_cart"),
        F.sum("is_cart").over(w2).alias("running_is_cart"),
        F.row_number().over(w3).alias("row_seq"),
        F.sum("is_cart").over(w4).alias("cumulative_is_cart"),
        F.lag("is_cart", 2, 0).over(w5).alias("prev_is_cart_2"),
    )
)
