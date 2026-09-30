from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["click", "add_to_cart", "purchase"]))
    .select("page")
    .orderBy(F.col("page").desc())
)
