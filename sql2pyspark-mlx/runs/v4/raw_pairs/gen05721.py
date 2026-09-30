from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id")
w2 = Window.partitionBy("event_type", "session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w4 = Window.partitionBy("page", "event_type").orderBy(F.col("next_duration_sec").desc_nulls_first())

valid = (
    df_web_events
    .filter(F.col("session_id").isin([4, 20, 39]))
    .withColumn("page_event_type", F.concat("page", F.lit(" "), "event_type"))
)

scored = (
    valid
    .withColumn("next_duration_sec", F.lead("duration_sec", 1).over(w))
    .withColumn("last_known_page", F.last("page", ignorenulls=True).over(w2))
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

deduped = (
    latest
    .withColumn("row_num_2", F.rank().over(w4))
    .filter(F.col("row_num_2") == 1)
)

joined = (
    deduped
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.country"), F.col("b.device"))
)

by_type = (
    joined
    .groupBy("country")
    .pivot("page", ["/checkout", "/cart"])
    .agg(F.avg("next_duration_sec"))
    .select(
        "country",
        F.col("/checkout").alias("checkout"),
        F.col("/cart").alias("cart"),
    )
)

result = (
    by_type
    .select("country", "cart")
    .orderBy(F.col("country").desc_nulls_last())
)
