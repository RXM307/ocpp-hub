"""Fake charger for testing the stack end to end (no hardware needed).

    pip install ocpp==1.0.0 websockets==14.1
    python simulate.py ws://localhost:9000/SIM001 [--charge]
Then press the "Charge" switch in Home Assistant (or pass --charge).
"""
import asyncio
import sys
from datetime import datetime, timezone

from ocpp.routing import on
from ocpp.v16 import ChargePoint, call, call_result
from websockets.asyncio.client import connect


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class Sim(ChargePoint):
    wh = 12_000
    charging = False

    async def status(self, st):
        await self.call(call.StatusNotification(connector_id=1, error_code="NoError", status=st, timestamp=now()))

    async def session(self, tag):
        await self.status("Preparing")
        res = await self.call(call.StartTransaction(connector_id=1, id_tag=tag, meter_start=self.wh, timestamp=now()))
        tx, self.charging = res.transaction_id, True
        await self.status("Charging")
        while self.charging:
            self.wh += 60
            await self.call(call.MeterValues(connector_id=1, transaction_id=tx, meter_value=[{
                "timestamp": now(), "sampledValue": [
                    {"value": str(self.wh), "measurand": "Energy.Active.Import.Register", "unit": "Wh"},
                    {"value": "7200", "measurand": "Power.Active.Import", "unit": "W"},
                    {"value": "31.3", "measurand": "Current.Import", "unit": "A", "phase": "L1"},
                    {"value": "230", "measurand": "Voltage", "unit": "V", "phase": "L1-N"}]}]))
            await asyncio.sleep(5)
        await self.call(call.StopTransaction(meter_stop=self.wh, timestamp=now(), transaction_id=tx))
        await self.status("Finishing")
        await asyncio.sleep(2)
        await self.status("Available")

    @on("RemoteStartTransaction")
    async def rs(self, id_tag, **kw):
        asyncio.create_task(self.session(id_tag))
        return call_result.RemoteStartTransaction(status="Accepted")

    @on("RemoteStopTransaction")
    async def rstop(self, **kw):
        self.charging = False
        return call_result.RemoteStopTransaction(status="Accepted")

    @on("SetChargingProfile")
    async def scp(self, **kw):
        return call_result.SetChargingProfile(status="Accepted")

    @on("ChangeConfiguration")
    async def cc(self, **kw):
        return call_result.ChangeConfiguration(status="Accepted")

    @on("GetConfiguration")
    async def gc(self, **kw):
        return call_result.GetConfiguration(configuration_key=[
            {"key": "MeterValueSampleInterval", "readonly": False, "value": "30"}])

    @on("TriggerMessage")
    async def tm(self, **kw):
        return call_result.TriggerMessage(status="Accepted")

    @on("UnlockConnector")
    async def uc(self, **kw):
        return call_result.UnlockConnector(status="Unlocked")

    @on("Reset")
    async def rst(self, **kw):
        return call_result.Reset(status="Accepted")


async def main(url, auto):
    async with connect(url, subprotocols=["ocpp1.6"]) as ws:
        cp = Sim(url.rsplit("/", 1)[-1], ws)
        runner = asyncio.create_task(cp.start())
        await cp.call(call.BootNotification(charge_point_model="Hub Pro (sim)", charge_point_vendor="Starcharge"))
        await cp.status("Available")
        if auto:
            asyncio.create_task(cp.session("SIMTAG"))
        await runner


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "ws://localhost:9000/SIM001", "--charge" in sys.argv))
