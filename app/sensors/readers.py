"""
Sensor readers for fluID.

Protocol abstraction layer — new protocols are added by subclassing BaseSensorReader.
Currently supports:
  - Modbus TCP (pymodbus)
  - Modbus RTU (serial, via pymodbus)
  - Simulated (for demo / shadow-mode testing)
"""

import asyncio
import random
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException

from app.db.models import SensorConfig, SensorProtocol

logger = logging.getLogger(__name__)

_sim_ticks: dict[str, int] = {}


@dataclass
class ReadResult:
    sensor_id: str
    sensor_key: str
    value: float
    raw_value: float
    quality: str
    timestamp: datetime
    error: Optional[str] = None


class BaseSensorReader(ABC):
    def __init__(self, config: SensorConfig):
        self.config = config

    @abstractmethod
    async def read(self) -> ReadResult:
        ...

    def _make_result(self, raw: float, quality: str = "GOOD",
                     error: str = None) -> ReadResult:
        value = raw * self.config.scale_factor
        value = max(self.config.min_range, min(self.config.max_range, value))
        return ReadResult(
            sensor_id=str(self.config.id),
            sensor_key=self.config.sensor_key,
            value=round(value, self.config.decimals),
            raw_value=raw,
            quality=quality,
            timestamp=datetime.utcnow(),
            error=error,
        )


class ModbusTCPReader(BaseSensorReader):
    TIMEOUT = 5

    async def read(self) -> ReadResult:
        client = AsyncModbusTcpClient(
            host=self.config.host,
            port=self.config.port,
            timeout=self.TIMEOUT,
        )
        try:
            await asyncio.wait_for(client.connect(), timeout=self.TIMEOUT)

            if not client.connected:
                return self._make_result(0, "BAD",
                    f"Cannot connect to {self.config.host}:{self.config.port}")

            response = await client.read_holding_registers(
                address=self.config.register_address,
                count=self.config.register_count,
                slave=self.config.unit_id,
            )

            if response.isError():
                return self._make_result(0, "BAD",
                    f"Modbus error on register {self.config.register_address}")

            raw = self._decode_registers(response.registers)
            return self._make_result(raw)

        except asyncio.TimeoutError:
            return self._make_result(0, "BAD",
                f"Timeout connecting to {self.config.host}:{self.config.port}")
        except ModbusException as e:
            return self._make_result(0, "BAD", f"Modbus exception: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error reading {self.config.sensor_key}")
            return self._make_result(0, "UNCERTAIN", str(e))
        finally:
            client.close()

    def _decode_registers(self, registers: list[int]) -> float:
        import struct
        if len(registers) >= 2:
            raw_bytes = struct.pack(">HH", registers[0], registers[1])
            return struct.unpack(">f", raw_bytes)[0]
        else:
            return float(registers[0])


class ModbusRTUReader(BaseSensorReader):
    async def read(self) -> ReadResult:
        from pymodbus.client import AsyncModbusSerialClient
        client = AsyncModbusSerialClient(
            port=self.config.host,
            baudrate=self.config.port or 9600,
            bytesize=8,
            parity="N",
            stopbits=1,
            timeout=5,
        )
        try:
            await client.connect()
            response = await client.read_holding_registers(
                address=self.config.register_address,
                count=self.config.register_count,
                slave=self.config.unit_id,
            )
            if response.isError():
                return self._make_result(0, "BAD", "Modbus RTU error")
            raw = float(response.registers[0])
            return self._make_result(raw)
        except Exception as e:
            return self._make_result(0, "BAD", str(e))
        finally:
            client.close()


class SimulatedReader(BaseSensorReader):
    async def read(self) -> ReadResult:
        key = str(self.config.id)
        tick = _sim_ticks.get(key, 0)
        _sim_ticks[key] = tick + 1

        prev = getattr(self, "_last_value", self.config.nominal_value)

        demo_spike = 0.0
        if self.config.sensor_key == "conductivity" and (tick % 80) > 60:
            demo_spike = ((tick % 80) - 60) * 0.015

        noise = random.gauss(0, 1) * (self.config.warn_threshold - self.config.nominal_value) * 0.04
        revert = (self.config.nominal_value - prev) * 0.05

        spike = 0.0
        if random.random() < 0.01:
            spike = random.gauss(0, 1) * (self.config.alert_threshold - self.config.nominal_value) * 0.3

        raw = prev + noise + revert + demo_spike + spike
        raw = max(self.config.min_range, min(self.config.max_range, raw))
        self._last_value = raw

        quality = "GOOD"
        if random.random() < 0.002:
            quality = "UNCERTAIN"

        return self._make_result(raw / self.config.scale_factor, quality)


class SensorReaderFactory:
    _registry = {
        SensorProtocol.MODBUS_TCP: ModbusTCPReader,
        SensorProtocol.MODBUS_RTU: ModbusRTUReader,
        SensorProtocol.SIMULATED: SimulatedReader,
    }

    @classmethod
    def create(cls, config: SensorConfig) -> BaseSensorReader:
        reader_class = cls._registry.get(config.protocol)
        if not reader_class:
            raise ValueError(f"No reader registered for protocol: {config.protocol}")
        return reader_class(config)

    @classmethod
    def register(cls, protocol: SensorProtocol, reader_class: type):
        cls._registry[protocol] = reader_class
