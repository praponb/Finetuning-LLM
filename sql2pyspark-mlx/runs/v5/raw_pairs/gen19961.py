from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(167, 201) & F.col("page").isin(["/search", "/product", "/cart"]) & (F.col("session_id") == 26))
    .select(
        "id",
        "duration_sec",
        "page",
        F.when(F.col("duration_sec") >= 471, "High").when(F.col("duration_sec") >= 169, "Medium").otherwise("Low").alias("duration_sec_band"),
    )
)
