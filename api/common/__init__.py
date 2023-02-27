from database.primary.common import *
from fastapi_restful.cbv import cbv
from api.common.models import *
from fastapi import APIRouter
from api import IoResource

router: APIRouter = APIRouter(prefix="/common")

@cbv(router)
class CommonRoutes(IoResource):
    @router.get("/devicetype", response_model=list[DeviceType], summary=["Device Type"])
    def get_device_type(self):
        return get_device_type(self.io)

    @router.get("/devices", response_model=list[Device], summary=["Device List"])
    def get_devices(self, devicetype: Optional[str] = None, deveui: Optional[str] = None, location: Optional[str] = None,
                    manufacturer: Optional[str] = None, store: Optional[str] = None):
        return get_devices(self.io, devicetype, deveui, location, manufacturer, store)

    @router.get("/eventtype", response_model=list[EventType], summary=["Events List"])
    def get_eventtype(self, devicetype: Optional[str] = None, eventtype: Optional[str] = None):
        return get_eventtype(self.io, devicetype, eventtype)

    @router.get("/deviceeventtype", response_model=list[DeviceEventType], summary=["Device Events List"])
    def get_device_event(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                                       eventdate_from: Optional[str] = None,
                                       eventdate_to: Optional[str] = None):
        return get_device_event(self.io, devicetype, deveui, eventdate_from, eventdate_to)

    @router.get("/aggregate/devicetype", response_model=list[DevicetypeActiveReported], summary=["Active and Reported device count for Device Type in last 30 days"])
    def get_aggregate_devicetype(self, devicetype: Optional[str] = None, eventdate_from: Optional[str] = None,
                                       eventdate_to: Optional[str] = None):
        return get_aggregate_devicetype(self.io, devicetype, eventdate_from, eventdate_to)

    @router.get("/aggregate/all", response_model=list[ActiveReported],
                summary=["Active, Reported, Active Alert device count"])
    def get_aggregate_all(self, eventdate_from: Optional[str] = None,
                                       eventdate_to: Optional[str] = None):
        return get_aggregate_all(self.io, eventdate_from, eventdate_to)

    @router.get("/aggregate/messagetype", response_model=list[ActiveReportedMessageType],
                summary=["Active, Reported, Active Alert device count by message type"])
    def get_aggregate_messagetype(self, eventdate_from: Optional[str] = None,
                          eventdate_to: Optional[str] = None):
        return get_aggregate_messagetype(self.io, eventdate_from, eventdate_to)

