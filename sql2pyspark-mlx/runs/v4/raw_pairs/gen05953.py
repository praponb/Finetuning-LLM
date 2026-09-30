from pyspark.sql import functions as F

result = (
    df_web_events
    .select("page")
    .subtract(
        df_web_events
        .filter(F.col("event_type").isin(["click", "search", "add_to_cart"]))
        .select("page")
    )
)
