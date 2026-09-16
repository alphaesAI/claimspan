from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, get_json_object, concat

class FilesToProcess:
    @staticmethod
    def process_bronze_member_stream(raw_df: DataFrame) -> DataFrame:
        """
        Applies transformation logic to incoming member records vectorially,
        dynamically deriving metadata (CLIENT_ID, FILE_LAYOUT_ID) from data/JSON streams.
        """
        # 1. Drop internal filler columns dynamically
        non_filler_cols = [c for c in raw_df.columns if not c.startswith("Filler_")]
        df_filtered = raw_df.select(non_filler_cols)

        file_id_expr = col("FILE_ID") if "FILE_ID" in df_filtered.columns else col("source_file_path")

        # 2. Extract dynamic metadata fields (falling back to defaults if null)
        client_id_col = col("CLIENT_ID") if "CLIENT_ID" in df_filtered.columns else lit("UNKNOWN_CLIENT")
        layout_id_col = col("FILE_LAYOUT_ID") if "FILE_LAYOUT_ID" in df_filtered.columns else lit("834")

        return (
            df_filtered
            .withColumn("CLIENT_ID", client_id_col)
            .withColumn("FILE_LAYOUT_ID", layout_id_col)
            .withColumn("FILE_LAYOUT_DESCRIPTION", concat(lit("Standard"), layout_id_col))
            .withColumn("FILE_ID", file_id_expr)
            .withColumn("LOAD_DATETIME", current_timestamp())
        )