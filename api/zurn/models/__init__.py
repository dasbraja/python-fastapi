from config import BaseModel
from typing import Optional, Union


class ZurnAlertStatus(BaseModel):
    devicetype: Optional[str]
    deveui: Optional[str]
    gatewayid: Optional[str]
    eventtime: Optional[str]
    eventdate: Optional[str]
    message_type: Optional[str]
    num_uses_tot: Optional[str]
    alertstatus: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)

class ZurnEventsTimeseries(BaseModel):
    devicetype: Optional[str]
    deveui: Optional[str]
    num_uses_tot: Optional[str]
    utime: Optional[str]
    diff_minutes: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)


class ZurnActivationDailyAggregate(BaseModel):
    devicetype: Optional[str]
    deveui: Optional[str]
    eventdate: Optional[str]
    activation_count: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)

class ZurnActivationAggregate(BaseModel):
    devicetype: Optional[str]
    deveui: Optional[str]
    alert_count: Optional[str]
    activation_count: Optional[str]
    daily_avg_activation_count: Optional[str]
    tot_activation_count: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)