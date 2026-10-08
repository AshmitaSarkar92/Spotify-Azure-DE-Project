from databricks.sdk.runtime import dbutils, display, spark

class reusable:
    def dropColumns(self, df, columns):
        df = df.drop(*columns)
        return df

    def previewStream(self, df, table_name):
        temp_path = f"abfss://silver@spotifyprojectdatalake.dfs.core.windows.net/{table_name}/_preview_delta"
        checkpoint_path = f"abfss://silver@spotifyprojectdatalake.dfs.core.windows.net/{table_name}/_preview_checkpoint"

        query = (
            df.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)
            .start(temp_path)
        )
        query.awaitTermination()

        display(df.sparkSession.read.format("delta").load(temp_path))

    def clearPreview(self, table_name):
        temp_path = f"abfss://silver@spotifyprojectdatalake.dfs.core.windows.net/{table_name}/_preview_delta"
        checkpoint_path = f"abfss://silver@spotifyprojectdatalake.dfs.core.windows.net/{table_name}/_preview_checkpoint"
        dbutils.fs.rm(temp_path, True)
        dbutils.fs.rm(checkpoint_path, True)