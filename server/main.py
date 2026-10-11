"""OCPP 1.6J central system that bridges a charger to Home Assistant via MQTT discovery.

Charger  --ws://host:9000/<id>-->  this server  --MQTT-->  Home Assistant
"""
import asyncio
import base64
import hmac
import json
import logging
import os
import re
import time
from datetime import datetime, timezone

import aiomqtt
from ocpp.routing import on
from ocpp.v16 import ChargePoint as BaseChargePoint
from ocpp.v16 import call, call_result
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed


def env(name, default=""):
    return os.environ.get(name, default)


def env_bool(name, default):
    return env(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


OCPP_HOST = env("OCPP_HOST", "0.0.0.0")
OCPP_PORT = 9000  # container-internal; change the published port in compose
OCPP_PASSWORD = env("OCPP_PASSWORD")
FORCE_ID = env("FORCE_STATION_ID")
MQTT_HOST = env("MQTT_HOST", "mosquitto")
MQTT_PORT = int(env("MQTT_PORT", "1883"))
MQTT_USER = env("MQTT_USERNAME") or None
MQTT_PASS = env("MQTT_PASSWORD") or None
BASE = env("MQTT_BASE_TOPIC", "ocpp").strip("/")
DISCOVERY = env("HA_DISCOVERY_PREFIX", "homeassistant").strip("/")
HEARTBEAT = int(env("HEARTBEAT_INTERVAL", "300"))
METER_INTERVAL = int(env("METER_INTERVAL", "30"))
AUTO_CONFIGURE = env_bool("AUTO_CONFIGURE", True)
AUTO_AUTHORIZE = env_bool("AUTO_AUTHORIZE", True)
AUTO_START = env_bool("AUTO_START_ON_PLUG", False)
DEFAULT_ID_TAG = env("DEFAULT_ID_TAG", "HOMEASSISTANT")[:20]
ALLOWED_TAGS = {t.strip() for t in env("ALLOWED_ID_TAGS").split(",") if t.strip()}
MIN_A = int(env("MIN_CURRENT", "6"))
MAX_A = int(env("MAX_CURRENT", "32"))
PROFILE_PURPOSE = env("PROFILE_PURPOSE", "TxDefaultProfile")
PROFILE_CONNECTOR = int(env("PROFILE_CONNECTOR", "1"))
PING = float(env("WS_PING_INTERVAL", "60"))

log = logging.getLogger("ocpp-hub")

PLUGGED = {"Preparing", "Charging", "SuspendedEV", "SuspendedEVSE", "Finishing"}
FULL_MEASURANDS = "Energy.Active.Import.Register,Power.Active.Import,Current.Import,Voltage"
MIN_MEASURANDS = "Energy.Active.Import.Register,Power.Active.Import"


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


# --------------------------------------------------------------------------- MQTT
class Bus:
    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue()

    def publish(self, topic, payload, retain=True):
        if not isinstance(payload, (str, bytes)):
            payload = json.dumps(payload)
        self.queue.put_nowait((topic, payload, retain))

    async def run(self):
        while True:
            try:
                will = aiomqtt.Will(f"{BASE}/bridge", "offline", retain=True)
                async with aiomqtt.Client(MQTT_HOST, MQTT_PORT, username=MQTT_USER,
                                          password=MQTT_PASS, will=will) as client:
                    log.info("MQTT connected to %s:%s", MQTT_HOST, MQTT_PORT)
                    await client.publish(f"{BASE}/bridge", "online", retain=True)
                    await client.subscribe(f"{BASE}/+/cmd/#")
                    await client.subscribe(f"{DISCOVERY}/status")
                    for st in stations.values():
                        st.publish_all()
                    async with asyncio.TaskGroup() as tg:
                        tg.create_task(self._writer(client))
                        tg.create_task(self._reader(client))
            except Exception as exc:  # noqa: BLE001 - reconnect on anything
                log.warning("MQTT error (%s); retrying in 5s", exc)
                await asyncio.sleep(5)

    async def _writer(self, client):
        while True:
            topic, payload, retain = await self.queue.get()
            await client.publish(topic, payload, retain=retain)

    async def _reader(self, client):
        async for msg in client.messages:
            topic = str(msg.topic)
            payload = msg.payload.decode() if isinstance(msg.payload, (bytes, bytearray)) else str(msg.payload)
            if topic == f"{DISCOVERY}/status":
                if payload == "online":  # HA restarted: re-send discovery
                    for st in stations.values():
                        st.publish_all()
                continue
            rest = topic[len(BASE) + 1:].split("/", 2)
            if len(rest) == 3 and rest[1] == "cmd" and rest[0] in stations:
                task = asyncio.create_task(stations[rest[0]].command(rest[2], payload))
                tasks.add(task)
                task.add_done_callback(tasks.discard)


bus = Bus()
stations: dict = {}
tasks: set = set()


# --------------------------------------------------------------------------- HA entities
def _num(unit, dclass, sclass="measurement", **kw):
    return {"unit_of_measurement": unit, "device_class": dclass, "state_class": sclass, **kw}


DIAG = {"entity_category": "diagnostic"}
ENTITIES = [
    ("sensor", "status", "Status", {"icon": "mdi:ev-station"}),
    ("sensor", "power", "Power", _num("W", "power")),
    ("sensor", "energy_total_kwh", "Total energy", _num("kWh", "energy", "total_increasing")),
    ("sensor", "session_energy_kwh", "Session energy", _num("kWh", "energy", "total_increasing")),
    ("sensor", "current", "Current", _num("A", "current")),
    ("sensor", "voltage", "Voltage", _num("V", "voltage")),
    ("sensor", "temperature", "Temperature", {**_num("°C", "temperature"), **DIAG}),
    ("sensor", "soc", "Vehicle SoC", _num("%", "battery")),
    ("sensor", "error_code", "Error code", DIAG),
    ("sensor", "station_status", "Station status", DIAG),
    ("sensor", "last_id_tag", "Last ID tag", DIAG),
    ("sensor", "last_seen", "Last message", {"device_class": "timestamp", **DIAG}),
    ("binary_sensor", "plugged_in", "Plugged in", {"device_class": "plug"}),
    ("binary_sensor", "charging", "Charging", {"device_class": "battery_charging"}),
    ("switch", "charge", "Charge", {"icon": "mdi:ev-plug-type2",
                                    "value_template": "{{ 'ON' if value_json.charging else 'OFF' }}"}),
    ("number", "current_limit", "Current limit", {"min": MIN_A, "max": MAX_A, "step": 1, "mode": "slider",
                                                  "unit_of_measurement": "A", "device_class": "current"}),
    ("button", "unlock", "Unlock connector", {"icon": "mdi:lock-open"}),
    ("button", "refresh", "Refresh status", {"icon": "mdi:refresh", **DIAG}),
    ("button", "reset", "Reboot charger", {"device_class": "restart", **DIAG}),
]
STATE_KEYS = ["status", "station_status", "error_code", "power", "energy_total_kwh", "session_energy_kwh",
              "current", "current_l1", "current_l2", "current_l3", "voltage", "temperature", "soc",
              "last_id_tag", "last_seen", "transaction_id"]


class Station:
    def __init__(self, ident: str):
        self.ident = ident
        self.sid = re.sub(r"[^A-Za-z0-9_-]", "_", ident)
        self.topic = f"{BASE}/{self.sid}"
        self.cp = None
        self.tx_id = None
        self.meter_start_wh = None
        self.info = {"vendor": None, "model": None, "firmware": None, "serial": None}
        self.config: dict = {}
        self.state = {k: None for k in STATE_KEYS}
        self.state.update(charging=False, plugged_in=False, current_limit=MAX_A)

    # ---- state
    def update(self, **kw):
        self.state.update(kw)
        if "status" in kw:
            st = kw["status"]
            self.state["charging"] = st == "Charging"
            self.state["plugged_in"] = st in PLUGGED
            if st != "Charging":
                self.state["power"] = 0
                self.state["current"] = 0
        self.publish_state()

    def touch(self):
        self.state["last_seen"] = now_iso()
        self.publish_state()

    def publish_state(self):
        bus.publish(f"{self.topic}/state", self.state)

    def publish_all(self):
        self.publish_discovery()
        self.publish_state()
        bus.publish(f"{self.topic}/availability", "online" if self.cp else "offline")
        if self.config:
            bus.publish(f"{self.topic}/config", self.config)

    def publish_discovery(self):
        device = {"identifiers": [f"ocpp_{self.sid}"], "name": f"EV Charger {self.ident}",
                  "manufacturer": self.info["vendor"] or "Starcharge / MG",
                  "model": self.info["model"] or "MG Charge Hub Pro"}
        if self.info["firmware"]:
            device["sw_version"] = self.info["firmware"]
        if self.info["serial"]:
            device["serial_number"] = self.info["serial"]
        for comp, key, name, extra in ENTITIES:
            cfg = {"name": name, "unique_id": f"ocpp_{self.sid}_{key}", "device": device,
                   "has_entity_name": True,
                   "availability": [{"topic": f"{self.topic}/availability"}, {"topic": f"{BASE}/bridge"}],
                   "availability_mode": "all", **extra}
            if comp != "button":
                cfg["state_topic"] = f"{self.topic}/state"
            if comp in ("sensor", "number"):
                cfg.setdefault("value_template", "{{ value_json.%s }}" % key)
            if comp == "binary_sensor":
                cfg.update(value_template="{{ value_json.%s }}" % key, payload_on="True", payload_off="False")
            if comp == "switch":
                cfg.update(payload_on="ON", payload_off="OFF")
            if comp in ("switch", "number", "button"):
                cfg["command_topic"] = f"{self.topic}/cmd/{key}"
            bus.publish(f"{DISCOVERY}/{comp}/ocpp_{self.sid}/{key}/config", cfg)

    # ---- connection lifecycle
    def attach(self, cp):
        if self.cp is not None and self.cp is not cp:
            task = asyncio.create_task(self.cp.raw_ws.close())  # stale connection
            tasks.add(task)
            task.add_done_callback(tasks.discard)
        self.cp = cp
        self.publish_all()

    def detach(self, cp):
        if self.cp is cp:
            self.cp = None
            bus.publish(f"{self.topic}/availability", "offline")

    # ---- HA -> charger
    async def command(self, name, payload):
        cp = self.cp
        if cp is None:
            log.warning("[%s] command %s ignored: charger not connected", self.ident, name)
            return
        log.info("[%s] command %s = %r", self.ident, name, payload)
        try:
            if name == "charge":
                await (cp.remote_start() if payload.strip().upper() in ("ON", "TRUE", "1") else cp.remote_stop())
            elif name == "current_limit":
                await cp.set_limit(max(MIN_A, min(MAX_A, int(float(payload)))))
            elif name == "unlock":
                await cp.send(call.UnlockConnector(connector_id=1))
            elif name == "reset":
                await cp.send(call.Reset(type="Soft"))
            elif name == "refresh":
                await cp.trigger()
        except Exception as exc:  # noqa: BLE001
            log.warning("[%s] command %s failed: %s", self.ident, name, exc)


def get_station(ident):
    sid = re.sub(r"[^A-Za-z0-9_-]", "_", ident)
    if sid not in stations:
        stations[sid] = Station(ident)
    return stations[sid]


# --------------------------------------------------------------------------- OCPP
class LoggingWS:
    """Wraps the websocket so every raw frame is logged and counts as 'seen'."""

    def __init__(self, ws, station):
        self.ws, self.station = ws, station

    async def recv(self):
        msg = await self.ws.recv()
        log.debug("[%s] <- %s", self.station.ident, msg)
        self.station.touch()
        return msg

    async def send(self, msg):
        log.debug("[%s] -> %s", self.station.ident, msg)
        await self.ws.send(msg)


def auth_status(tag):
    ok = AUTO_AUTHORIZE or tag == DEFAULT_ID_TAG or tag in ALLOWED_TAGS
    return "Accepted" if ok else "Invalid"


class Charger(BaseChargePoint):
    def __init__(self, station: Station, ws):
        super().__init__(station.ident, LoggingWS(ws, station))
        self.station, self.raw_ws = station, ws
        self._setup_done = False
        self._booted = False

    # ---- helpers for outgoing calls
    async def send(self, payload):
        res = await self.call(payload)
        log.info("[%s] %s -> %s", self.station.ident, type(payload).__name__, getattr(res, "status", res))
        return res

    async def remote_start(self):
        res = await self.send(call.RemoteStartTransaction(id_tag=DEFAULT_ID_TAG, connector_id=1))
        if res.status != "Accepted":
            log.warning("[%s] RemoteStart rejected (status=%s, plugged_in=%s). Retrying without connectorId",
                        self.station.ident, self.station.state.get("status"), self.station.state.get("plugged_in"))
            await self.send(call.RemoteStartTransaction(id_tag=DEFAULT_ID_TAG))

    async def remote_stop(self):
        if self.station.tx_id is None:
            log.warning("[%s] no active transaction to stop", self.station.ident)
            return
        await self.send(call.RemoteStopTransaction(transaction_id=self.station.tx_id))

    async def set_limit(self, amps):
        profile = {"chargingProfileId": 1, "stackLevel": 0, "chargingProfilePurpose": PROFILE_PURPOSE,
                   "chargingProfileKind": "Absolute",
                   "chargingSchedule": {"chargingRateUnit": "A",
                                        "chargingSchedulePeriod": [{"startPeriod": 0, "limit": float(amps)}]}}
        res = await self.send(call.SetChargingProfile(connector_id=PROFILE_CONNECTOR, cs_charging_profiles=profile))
        if res.status == "Accepted":
            self.station.update(current_limit=amps)

    async def trigger(self):
        for msg in ("StatusNotification", "MeterValues"):
            try:
                await self.send(call.TriggerMessage(requested_message=msg, connector_id=1))
            except Exception as exc:  # noqa: BLE001
                log.info("[%s] TriggerMessage %s failed: %s", self.station.ident, msg, exc)

    async def _set_config(self, key, value):
        res = await self.send(call.ChangeConfiguration(key=key, value=value))
        return res.status in ("Accepted", "RebootRequired")

    async def post_connect(self):
        """This charger may never send BootNotification; set up anyway if it hasn't within 8s."""
        await asyncio.sleep(8)
        if self._booted or self._setup_done:
            return
        log.info("[%s] no BootNotification seen; requesting one via TriggerMessage", self.station.ident)
        try:
            await self.send(call.TriggerMessage(requested_message="BootNotification"))
        except Exception as exc:  # noqa: BLE001
            log.info("[%s] TriggerMessage BootNotification failed: %s", self.station.ident, exc)
        await asyncio.sleep(6)
        if not self._setup_done:
            log.info("[%s] still no BootNotification; running setup anyway", self.station.ident)
            await self.post_boot()

    async def post_boot(self):
        """Dump config, tune metering, request status (once per connection)."""
        if self._setup_done:
            return
        self._setup_done = True
        await asyncio.sleep(2)
        try:
            res = await self.send(call.GetConfiguration())
            cfg = {k["key"]: k.get("value") for k in (res.configuration_key or [])}
            self.station.config = cfg
            bus.publish(f"{self.station.topic}/config", cfg)
            log.info("[%s] charger configuration: %s", self.station.ident, json.dumps(cfg))
            if AUTO_CONFIGURE:
                await self._set_config("MeterValueSampleInterval", str(METER_INTERVAL))
                if not await self._set_config("MeterValuesSampledData", FULL_MEASURANDS):
                    await self._set_config("MeterValuesSampledData", MIN_MEASURANDS)
            await self.trigger()
        except Exception as exc:  # noqa: BLE001
            log.warning("[%s] post-boot setup incomplete: %s", self.station.ident, exc)

    async def _auto_start(self):
        await asyncio.sleep(2)
        if self.station.tx_id is None:
            await self.remote_start()

    # ---- charger -> server
    @on("BootNotification")
    async def on_boot(self, charge_point_model, charge_point_vendor, **kw):
        s = self.station
        self._booted = True
        s.info.update(vendor=charge_point_vendor, model=charge_point_model,
                      firmware=kw.get("firmware_version"), serial=kw.get("charge_point_serial_number"))
        log.info("[%s] boot: %s %s fw=%s", s.ident, charge_point_vendor, charge_point_model, s.info["firmware"])
        s.publish_discovery()
        asyncio.create_task(self.post_boot())
        return call_result.BootNotification(current_time=now_iso(), interval=HEARTBEAT, status="Accepted")

    @on("Heartbeat")
    async def on_heartbeat(self, **kw):
        return call_result.Heartbeat(current_time=now_iso())

    @on("StatusNotification")
    async def on_status(self, connector_id, error_code, status, **kw):
        s = self.station
        if connector_id == 0:
            s.update(station_status=status)
        elif connector_id == 1:
            s.update(status=status, error_code=error_code)
            if AUTO_START and status == "Preparing" and s.tx_id is None:
                asyncio.create_task(self._auto_start())
        else:
            log.info("[%s] status for connector %s: %s", s.ident, connector_id, status)
        return call_result.StatusNotification()

    @on("Authorize")
    async def on_authorize(self, id_tag, **kw):
        self.station.update(last_id_tag=id_tag)
        return call_result.Authorize(id_tag_info={"status": auth_status(id_tag)})

    @on("StartTransaction")
    async def on_start(self, connector_id, id_tag, meter_start, **kw):
        s = self.station
        s.tx_id = int(time.time()) & 0x7FFFFFFF
        s.meter_start_wh = meter_start
        s.update(last_id_tag=id_tag, transaction_id=s.tx_id, session_energy_kwh=0.0)
        return call_result.StartTransaction(transaction_id=s.tx_id,
                                            id_tag_info={"status": auth_status(id_tag)})

    @on("StopTransaction")
    async def on_stop(self, meter_stop, transaction_id, **kw):
        s = self.station
        ups = {"power": 0, "current": 0, "transaction_id": None, "energy_total_kwh": round(meter_stop / 1000, 3)}
        if s.meter_start_wh is not None:
            ups["session_energy_kwh"] = round(max(0, meter_stop - s.meter_start_wh) / 1000, 3)
        s.tx_id, s.meter_start_wh = None, None
        s.update(**ups)
        return call_result.StopTransaction(id_tag_info={"status": "Accepted"})

    @on("MeterValues")
    async def on_meter(self, connector_id, meter_value, transaction_id=None, **kw):
        s = self.station
        if transaction_id and s.tx_id is None and s.state["status"] == "Charging":
            s.tx_id = transaction_id  # adopt after a server restart mid-session
            s.state["transaction_id"] = transaction_id
        power, currents, volts, out = {}, {}, [], {}
        for mv in meter_value or []:
            for sv in mv.get("sampled_value") or []:
                if sv.get("format") == "SignedData":
                    continue
                try:
                    val = float(sv.get("value"))
                except (TypeError, ValueError):
                    continue
                meas = sv.get("measurand") or "Energy.Active.Import.Register"
                unit = (sv.get("unit") or "").lower()
                phase = (sv.get("phase") or "total")[:2].upper() if sv.get("phase") else "total"
                if meas == "Energy.Active.Import.Register":
                    wh = val * 1000 if unit == "kwh" else val
                    out["energy_total_kwh"] = round(wh / 1000, 3)
                    if s.meter_start_wh is not None:
                        out["session_energy_kwh"] = round(max(0, wh - s.meter_start_wh) / 1000, 3)
                elif meas == "Power.Active.Import":
                    power[phase] = val * 1000 if unit == "kw" else val
                elif meas == "Current.Import":
                    currents[phase] = val
                elif meas == "Voltage":
                    volts.append(val)
                elif meas == "Temperature":
                    out["temperature"] = val
                elif meas == "SoC":
                    out["soc"] = val
        if power:
            out["power"] = round(power["total"] if "total" in power else sum(power.values()), 1)
        if currents:
            for i in ("L1", "L2", "L3"):
                if i in currents:
                    out[f"current_{i.lower()}"] = currents[i]
            phases = [currents[i] for i in ("L1", "L2", "L3") if i in currents]
            out["current"] = currents["total"] if "total" in currents else (max(phases) if phases else None)
        if volts:
            out["voltage"] = round(sum(volts) / len(volts), 1)
        if out:
            s.update(**out)
        return call_result.MeterValues()

    @on("DataTransfer")
    async def on_data_transfer(self, vendor_id, **kw):
        log.info("[%s] DataTransfer from vendor %s: %s", self.station.ident, vendor_id, kw)
        return call_result.DataTransfer(status="UnknownVendorId")

    @on("DiagnosticsStatusNotification")
    async def on_diag(self, status, **kw):
        return call_result.DiagnosticsStatusNotification()

    @on("FirmwareStatusNotification")
    async def on_fw(self, status, **kw):
        return call_result.FirmwareStatusNotification()


def authorised(ws):
    if not OCPP_PASSWORD:
        return True
    header = ws.request.headers.get("Authorization", "")
    if not header.startswith("Basic "):
        return False
    try:
        _, _, password = base64.b64decode(header[6:]).decode().partition(":")
    except Exception:  # noqa: BLE001
        return False
    return hmac.compare_digest(password, OCPP_PASSWORD)


async def on_connect(ws):
    path = ws.request.path.split("?")[0]
    ident = FORCE_ID or ([p for p in path.split("/") if p] or ["charger"])[-1]
    log.info("connection from %s path=%s id=%s requested-subprotocols=%s negotiated=%s",
             ws.remote_address, path, ident, ws.request.headers.get("Sec-WebSocket-Protocol"), ws.subprotocol)
    if not authorised(ws):
        log.warning("[%s] rejected: bad credentials", ident)
        await ws.close(1008, "unauthorized")
        return
    station = get_station(ident)
    cp = Charger(station, ws)
    station.attach(cp)
    asyncio.create_task(cp.post_connect())
    try:
        await cp.start()
    except ConnectionClosed:
        pass
    except Exception:  # noqa: BLE001
        log.exception("[%s] connection error", ident)
    finally:
        log.info("[%s] disconnected", ident)
        station.detach(cp)


async def main():
    logging.basicConfig(level=env("LOG_LEVEL", "INFO").upper(),
                        format="%(asctime)s %(levelname)s %(message)s")
    for noisy in ("websockets", "ocpp", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    # Port scanners / browsers / health checks hitting :9000 are not chargers; don't spam tracebacks.
    logging.getLogger("websockets.server").setLevel(logging.CRITICAL)
    bus_task = asyncio.create_task(bus.run())
    async with serve(on_connect, OCPP_HOST, OCPP_PORT, subprotocols=["ocpp1.6"],
                     ping_interval=PING or None, ping_timeout=PING or None):
        log.info("OCPP 1.6J server listening on %s:%s", OCPP_HOST, OCPP_PORT)
        await bus_task


if __name__ == "__main__":
    asyncio.run(main())
