from config import BaseModel
from typing import Optional, Union


class GPProductAggregate(BaseModel):
    devicetype: Optional[str]
    deveui: Optional[str]
    alert_resolved_count: Optional[str]
    alert_sent_count: Optional[str]
    product_usage: Optional[str]
    dispense_count: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)


class GPAlertStatus(BaseModel):
    devicetype: Optional[str]
    deveui: Optional[str]
    gatewayid: Optional[str]
    recordnumber: Optional[str]
    eventtime: Optional[str]
    eventdate: Optional[str]
    alertname: Optional[str]
    rdata: Optional[str]
    alertstatus: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)


class GPDailyAlertStatus(BaseModel):
    devicetype: Optional[str]
    deveui: Optional[str]
    eventdate: Optional[str]
    alertname: Optional[str]
    roll: Optional[str]
    roll0_alert_count: Optional[str]
    roll1_alert_count: Optional[str]
    roll0_resolved_count: Optional[str]
    roll1_resolved_count: Optional[str]
    roll0_sent_count: Optional[str]
    roll1_sent_count: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)

class GPDailyProductUsage(BaseModel):
    devicetype: Optional[str]
    restroom_location: Optional[str]
    store: Optional[str]
    deveui: Optional[str]
    eventdate: Optional[str]
    roll0_product_usage: Optional[str]
    roll1_product_usage: Optional[str]
    product_usage: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)

class GPDailyDispense(BaseModel):
    devicetype: Optional[str]
    deveui: Optional[str]
    eventdate: Optional[str]
    dispense_count: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)

class GPDispenseReport(BaseModel):
    devicetype: Optional[str]
    deveui: Optional[str]
    eventdate: Optional[str]
    product_usage: Optional[str]
    dispense_count: Optional[str]
    recordfrom: Optional[str]
    recordto: Optional[str]


    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)


class GPFuelGauge(BaseModel):
    devicetype: Optional[str]
    deveui: Optional[str]
    eventtime: Optional[str]
    eventdate: Optional[str]
    gatewayid: Optional[str]
    recordnumber: Optional[str]
    rparam: Optional[str]
    roll: Optional[str]
    remaining: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)

class GPBatteryFuelGauge(BaseModel):
    devicetype: Optional[str]
    deveui: Optional[str]
    eventtime: Optional[str]
    eventdate: Optional[str]
    battery_reading: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)


class GPDailyBatteryAlert(BaseModel):
    devicetype: Optional[str]
    deveui: Optional[str]
    eventdate: Optional[str]
    normal_count: Optional[str]
    low_count: Optional[str]
    shutdown_count: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)

class GPBatteryCard(BaseModel):
    devicetype: Optional[str]
    deveui: Optional[str]
    normal_count: Optional[str]
    low_count: Optional[str]
    shutdown_count: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)


class GPDispenserPowerup(BaseModel):
    devicetype: Optional[str]
    deveui: Optional[str]
    eventtime: Optional[str]
    eventdate: Optional[str]
    eventhour: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)

class GPDispenserCoverEventLag(BaseModel):
    devicetype: Optional[str]
    deveui: Optional[str]
    recordnumber: Optional[str]
    eventtime: Optional[str]
    eventtype: Optional[str]
    rdata: Optional[str]
    lead_rdata: Optional[str]
    second_diff: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)


class GPDispenserCoverEventAgg(BaseModel):
    devicetype: Optional[str]
    deveui: Optional[str]
    count: Optional[str]
    avg_second_diff: Optional[str]

    class Config:
        orm_mode = True

    def dict(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return BaseModel.dict(self, *args, **kwargs)

