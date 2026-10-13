# OCPP 1.6J server + Home Assistant (MQTT discovery) for MG Charge Hub Pro / Starcharge

Charger --ws--> `ocpp-server` (this repo) --MQTT--> Home Assistant (entities appear automatically)

## Quick start
1. `cp .env.example .env` and set `MQTT_HOST` (and credentials) to the broker Home Assistant uses.
   - No broker yet? Set `MQTT_HOST=mosquitto`, run `docker compose --profile broker up -d --build`,
     then add the MQTT integration in HA pointing at this host:1883.
   - Existing broker: `docker compose up -d --build`
2. Test without hardware: `python tools/simulate.py ws://<server-ip>:9000/SIM001 --charge`
   A device "EV Charger SIM001" should appear in HA with live power/energy.
3. Point the real charger at `ws://<server-ip>:9000/<station-id>` (OCPP 1.6J, no TLS).
   Watch `docker compose logs -f ocpp-server` (LOG_LEVEL=DEBUG shows every raw frame).

## Entities created
Status, Power, Current, Voltage, Session/Total energy (Energy dashboard compatible), Temperature, SoC,
Plugged in, Charging, **Charge** switch (RemoteStart/Stop), **Current limit** slider (SetChargingProfile),
Unlock connector, Refresh, Reboot charger, plus diagnostics (error code, last ID tag, last message).
The charger's full OCPP configuration is published retained to `ocpp/<id>/config` and logged on boot.

## Troubleshooting
- Charger never connects: check URL/port/firewall, and that the log shows a "connection from..." line.
  The log prints the subprotocol the charger asked for; it must include `ocpp1.6`.
- Connects but no power/energy: look at the logged GetConfiguration keys and MeterValues frames;
  set `METER_INTERVAL` / check `MeterValuesSampledData` was accepted.
- Charge switch does nothing: charger may need an authorised tag first (`AUTO_AUTHORIZE=true`) or
  `AUTO_START_ON_PLUG=true`. Current limit rejected: try `PROFILE_PURPOSE=ChargePointMaxProfile`, `PROFILE_CONNECTOR=0`.
- Keep this on your LAN/VPN. Set `OCPP_PASSWORD` if the charger supports Basic auth.

## Starcharge Artemis_CS notes (from real logs)
- `FreeCharging=1` makes the charger reject every RemoteStartTransaction ("FreeCharging mode is enabled,
  remote start is not allowed"). `FORCE_CONFIG=FreeCharging=0` (default) switches it off on connect.
- Any config key can be changed from HA/MQTT: publish `FreeCharging=0` to `ocpp/<id>/cmd/set_config`.
- `OfflinePlugChargeEnabled=true` makes the charger fall back to free charging if the server is unreachable.

- A leftover charging profile with a 0 A limit (seen as `limit:0.0A` / `OCPPCurrent[0]` in the charger log) lets a
  remote start succeed but the charger never offers power (status SuspendedEV/SuspendedEVSE, PWM 0.0).
  `CLEAR_PROFILES_ON_CONNECT=true` (default) or the "Clear charging limits" button removes it.

## Status
Written against the OCPP 1.6J spec; **not yet tested against a real MG Charge Hub Pro** (or run in CI).
