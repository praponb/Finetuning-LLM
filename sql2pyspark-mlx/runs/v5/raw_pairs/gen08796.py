from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/cart", "/checkout"]))
    .select(
        "id",
        "page",
        F.concat("event_type", F.lit(" "), "page").alias("event_type_page"),
        F.to_date("event_time").alias("event_time_date"),
    )
)
