from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/cart", "/checkout"]))
    .select("id", "duration_sec")
    .unionByName(
        df_web_events
        .filter(F.col("duration_sec") < 290)
        .select("id", "duration_sec")
    )
    .distinct()
    .orderBy("duration_sec")
)
