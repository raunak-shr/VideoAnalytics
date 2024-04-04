from pydantic import BaseModel


class IpModel(BaseModel):
    file: str
    threshold: float

#
# class IpRTSPModel(BaseModel):
#     stream_link: str
#     threshold: float
