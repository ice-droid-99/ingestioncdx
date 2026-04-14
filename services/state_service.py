from utils.s3_io import s3_read_json, s3_write_json

class StateService:
    def __init__(self, watermark_state_uri: str):
        if not watermark_state_uri.startswith("s3://"):
            raise ValueError("StateService expects an S3 URI (s3://...) for ECS mode.")
        self.watermark_state_uri = watermark_state_uri
        self.state = s3_read_json(watermark_state_uri, default={})

    def get_watermark(self, table: str):
        return self.state.get(table)

    def set_watermark(self, table: str, watermark_value: str):
        self.state[table] = watermark_value
        s3_write_json(self.watermark_state_uri, self.state)