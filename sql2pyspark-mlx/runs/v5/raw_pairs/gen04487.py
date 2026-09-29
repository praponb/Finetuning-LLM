from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/product", "/checkout", "/home"]))
    .select("event_type")
    .unionByName(
        df_web_events
        .filter(F.col("event_type") == "add_to_cart")
        .select("event_type")
    )
    .distinct()
    .orderBy("event_type")
)
