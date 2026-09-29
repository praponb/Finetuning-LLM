from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .pivot("page", ["/home", "/product", "/cart"])
    .agg(F.avg("duration_sec"))
    .select(
        "session_id",
        F.col("/home").alias("home"),
        F.col("/product").alias("product"),
        F.col("/cart").alias("cart"),
    )
)
