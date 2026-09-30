from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("page").isin(["/product", "/cart", "/checkout"]))
    .select("id", "event_type")
    .unionByName(
        df_web_events
        .filter(F.col("event_type") == "add_to_cart")
        .select("id", "event_type")
    )
)
