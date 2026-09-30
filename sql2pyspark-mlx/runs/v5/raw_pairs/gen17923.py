from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Safari", "Chrome", "Edge"]))
    .select("country")
    .unionByName(
        df_sessions
        .filter(F.col("country").isin(["SG", "TH", "JP"]))
        .select("country")
    )
    .orderBy("country")
)
