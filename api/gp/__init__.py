from database.primary.gp import *
from fastapi_restful.cbv import cbv
from api.gp.models import *
from fastapi import APIRouter
from api import IoResource

router: APIRouter = APIRouter(prefix="/gp")

@cbv(router)
class GPRoutes(IoResource):

    @router.get("/aggregate/productcard", response_model=list[GPProductAggregate],
                summary=["Dispenser Product Analytics Card (Date format: YYYY-MM-DD)"])
    def get_aggregate_product(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                              roll: Optional[str] = None,
                                       eventdate_from: Optional[str] = None,
                                       eventdate_to: Optional[str] = None):
        return get_aggregate_product(self.io, devicetype, deveui, roll, eventdate_from, eventdate_to)

    @router.get("/aggregate/batterycard", response_model=list[GPBatteryCard],
                summary=["Dispenser Battery Card (Date format: YYYY-MM-DD)"])
    def get_battery_card(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                              eventdate_from: Optional[str] = None,
                              eventdate_to: Optional[str] = None):
        return get_battery_card(self.io, devicetype, deveui, eventdate_from, eventdate_to)

    @router.get("/aggregate/daily/productalert", response_model=list[GPDailyAlertStatus],
                summary=["Dispenser Daily Product Alert Summary (Date format: YYYY-MM-DD)"])
    def get_daily_productalert_summary(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                                       roll: Optional[str] = None,
                                       eventdate_from: Optional[str] = None,
                                       eventdate_to: Optional[str] = None):
        return get_daily_productalert_summary(self.io, devicetype, deveui, roll, eventdate_from, eventdate_to)

    @router.get("/aggregate/daily/productusage", response_model=list[GPDailyProductUsage],
                summary=["Dispenser Daily Product Usage Summary (Date format: YYYY-MM-DD)"])
    def get_daily_productusage_summary(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                                       eventdate_from: Optional[str] = None,
                                       eventdate_to: Optional[str] = None):
        return get_daily_productusage_summary(self.io, devicetype, deveui, eventdate_from, eventdate_to)

    @router.get("/aggregate/daily/dispense", response_model=list[GPDailyDispense],
                summary=["Dispenser Daily Dispense Summary (Date format: YYYY-MM-DD)"])
    def get_daily_dispense_summary(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                                       eventdate_from: Optional[str] = None,
                                       eventdate_to: Optional[str] = None):
        return get_daily_dispense_summary(self.io, devicetype, deveui, eventdate_from, eventdate_to)

    @router.get("/aggregate/daily/batteryalert", response_model=list[GPDailyBatteryAlert],
                summary=["Dispenser Daily Battery Alert Summary (Date format: YYYY-MM-DD)"])
    def get_daily_batteryalert_summary(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                                       eventdate_from: Optional[str] = None,
                                       eventdate_to: Optional[str] = None):
        return get_daily_batteryalert_summary(self.io, devicetype, deveui, eventdate_from, eventdate_to)

    @router.get("/aggregate/covereventlag", response_model=list[GPDispenserCoverEventAgg],
                summary=["Dispenser Cover Event Lag Aggregate (Date format: YYYY-MM-DD)"])
    def get_dispenser_coverevent_agg(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                                     eventdate_from: Optional[str] = None, eventdate_to: Optional[str] = None):
        return get_dispenser_coverevent_agg(self.io, devicetype, deveui, eventdate_from, eventdate_to)


    @router.get("/productalert", response_model=list[GPAlertStatus], summary=["Dispenser Product Low or Empty Alerts (Date format: YYYY-MM-DD)"])
    def gp_alert_statuses(self, devicetype: Optional[str] = None, deveui: Optional[str] = None, alertname: Optional[str] = None,
                          alertstatus: Optional[str] = None,  eventdate_from: Optional[str] = None,  eventdate_to: Optional[str] = None):
        return get_alert_status(self.io, devicetype, deveui, alertname, alertstatus, eventdate_from, eventdate_to)


    @router.get("/{deveui}/productalert", response_model=list[GPAlertStatus], summary=["Dispenser Product Low or Empty Alerts by Device (Date format: YYYY-MM-DD)"])
    def gp_device_alert_status(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                          alerttype: Optional[str] = None, eventdate_from: Optional[str] = None,  eventdate_to: Optional[str] = None):
        return get_device_alert_status(self.io, deveui, alerttype, eventdate_from, eventdate_to)



    @router.get("/batteryalert", response_model=list[GPAlertStatus],
                summary=["Dispenser Battery Low, Shutdown Alerts (Date format: YYYY-MM-DD)"])
    def get_battery_alert(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                          alertname: Optional[str] = None,
                          alertstatus: Optional[str] = None, eventdate_from: Optional[str] = None,
                          eventdate_to: Optional[str] = None):
        return get_battery_alert(self.io, devicetype, deveui, alertname, alertstatus, eventdate_from, eventdate_to)

    @router.get("/{deveui}/batteryalert", response_model=list[GPAlertStatus],
                summary=["Dispenser Battery Low, Shutdown Alerts by Device (Date format: YYYY-MM-DD)"])
    def get_device_battery_alert(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                               alerttype: Optional[str] = None, alertstatus: Optional[str] = None,
                                 eventdate_from: Optional[str] = None,
                               eventdate_to: Optional[str] = None):
        return get_device_battery_alert(self.io, deveui, alerttype, alertstatus, eventdate_from, eventdate_to)


    @router.get("/dispensereport", response_model=list[GPDispenseReport], summary=["Dispense Report  (Date format: YYYY-MM-DD)"])
    def get_dispense_report(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                          eventdate: Optional[str] = None):
        return get_dispense_report(self.io, devicetype, deveui, eventdate)

    @router.get("/{deveui}/dispensehistory", response_model=list[GPDispenseReport], summary=["Dispense History by Device  (Date format: YYYY-MM-DD)"])
    def get_device_dispense_history(self, deveui: Optional[str] = None,
                          eventdate_from: Optional[str] = None, eventdate_to: Optional[str] = None):
        return get_device_dispense_history(self.io, deveui, eventdate_from, eventdate_to)

    @router.get("/productfuelgaugehistory", response_model=list[GPFuelGauge],
                summary=["Product Fuel gauge History (Date format: YYYY-MM-DD)"])
    def get_fuelgauge_hist(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                                    eventdate_from: Optional[str] = None, eventdate_to: Optional[str] = None,
                           remaining: Optional[str] = None, roll: Optional[str] = None):
        return get_fuelgauge_hist(self.io, devicetype, deveui, eventdate_from, eventdate_to, remaining, roll)

    @router.get("/{deveui}/productfuelgaugehistory", response_model=list[GPFuelGauge],
                summary=["Product Fuel gauge History (Date format: YYYY-MM-DD)"])
    def get_device_fuelgauge_hist(self, deveui: Optional[str] = None,
                           eventdate_from: Optional[str] = None, eventdate_to: Optional[str] = None,
                           remaining: Optional[str] = None):
        return get_device_fuelgauge_hist(self.io, deveui, eventdate_from, eventdate_to, remaining)

    @router.get("/batteryfuelgaugehistory", response_model=list[GPBatteryFuelGauge],
                summary=["Battery Fuel gauge History (Date format: YYYY-MM-DD)"])
    def get_battery_fuelgauge_hist(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                                    eventdate_from: Optional[str] = None, eventdate_to: Optional[str] = None):
        return get_battery_fuelgauge_hist(self.io, devicetype, deveui, eventdate_from, eventdate_to)


    @router.get("/dispenserpowerup", response_model=list[GPDispenserPowerup],
                summary=["Dispenser Powerup Events (Date format: YYYY-MM-DD)"])
    def get_dispenser_powerup_hist(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                                   eventdate_from: Optional[str] = None, eventdate_to: Optional[str] = None):
        return get_dispenser_powerup_hist(self.io, devicetype, deveui, eventdate_from, eventdate_to)

    @router.get("/covereventlag", response_model=list[GPDispenserCoverEventLag],
                summary=["Dispenser Cover Event Lag Events (Date format: YYYY-MM-DD)"])
    def get_dispenser_coverevent_lag(self, devicetype: Optional[str] = None, deveui: Optional[str] = None,
                                   eventdate_from: Optional[str] = None, eventdate_to: Optional[str] = None):
        return get_dispenser_coverevent_lag(self.io, devicetype, deveui, eventdate_from, eventdate_to)

