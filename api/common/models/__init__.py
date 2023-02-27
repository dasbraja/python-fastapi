from config import BaseModel
from typing import Optional, Union


class DeviceType(BaseModel):
    devicetype: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)

class Device(BaseModel):
    devicetype: Optional[str]
    deveui: Optional[str]
    location: Optional[str]
    manufacturer : Optional[str]
    store: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)

class EventType(BaseModel):
    eventtype: Optional[str]
    devicetype: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)

class DeviceEventType(BaseModel):
    recordid: Optional[str]
    devicetype: Optional[str]
    deveui: Optional[str]
    eventtype: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)

class DevicetypeActiveReported(BaseModel):
    devicetype: Optional[str]
    reported_device_count: Optional[str]
    active_device_count: Optional[str]
    active_alert_device_count: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)

class ActiveReported(BaseModel):
    reported_device_count: Optional[str]
    active_device_count: Optional[str]
    active_alert_device_count: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)


class ActiveReportedMessageType(BaseModel):
    message_type: Optional[str]
    soap_dispenser_count: Optional[str]
    papertowel_dispenser_count: Optional[str]
    toiletpaper_dispenser_count: Optional[str]
    flushvalve_count: Optional[str]
    faucet_count: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)