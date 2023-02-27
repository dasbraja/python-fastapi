from database.primary.zurn import *
from fastapi_restful.cbv import cbv
from api.zurn.models import *
from fastapi import APIRouter
from api import IoResource

router: APIRouter = APIRouter(prefix="/zurn")

@cbv(router)
class ZurnRoutes(IoResource):

    @router.get("/events", response_model=list[ZurnEventsTimeseries],
                summary=["Zurn Valve Events (Date format: YYYY-MM-DD)"])
    def get_events_timeseries(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                          eventdate_from: Optional[str] = None, eventdate_to: Optional[str] = None):
        return get_events_timeseries(self.io, devicetype, deveui, eventdate_from, eventdate_to)

    @router.get("/alertstatus", response_model=list[ZurnAlertStatus], summary=["Zurn Valve Alerts (Date format: YYYY-MM-DD)"])
    def gp_alert_statuses(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                          eventdate_from: Optional[str] = None, eventdate_to: Optional[str] = None):
        return get_alert_status(self.io, devicetype, deveui, eventdate_from, eventdate_to)

    @router.get("/{deveui}/alertstatus", response_model=list[ZurnAlertStatus], summary=["Zurn Valve Alerts by device (Date format: YYYY-MM-DD)"])
    def gp_device_alert_statuses(self, deveui: Optional[str] = None,
                                 eventdate_from: Optional[str] = None, eventdate_to: Optional[str] = None):
        return get_device_alert_status(self.io, deveui, eventdate_from, eventdate_to)

    @router.get("/aggregate/daily/activation", response_model=list[ZurnActivationDailyAggregate],
                summary=["Zurn Daily Activation Aggregate (Date format: YYYY-MM-DD)"])
    def get_daily_aggregate_activation(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                          eventdate_from: Optional[str] = None, eventdate_to: Optional[str] = None):
        return get_daily_aggregate_activation(self.io, devicetype, deveui, eventdate_from, eventdate_to)

    @router.get("/aggregate/activation", response_model=list[ZurnActivationAggregate],
                summary=["Zurn Activation Aggregate (Date format: YYYY-MM-DD)"])
    def get_aggregate_activation(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                                       eventdate_from: Optional[str] = None, eventdate_to: Optional[str] = None):
        return get_aggregate_activation(self.io, devicetype, deveui, eventdate_from, eventdate_to)

