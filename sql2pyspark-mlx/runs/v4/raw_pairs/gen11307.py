from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .select("id", "event_type", "page")
    .intersect(
        df_web_events
        .filter(F.col("page").isin(["/search", "/product", "/cart"]))
        .select("id", "event_type", "page")
    )
)
