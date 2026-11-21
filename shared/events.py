"""Flow event schema shared by the collector and the feature engine. One event per packet."""

from __future__ import annotations

from enum import IntEnum, IntFlag

from pydantic import BaseModel, ConfigDict, Field, IPvAnyAddress


class Proto(IntEnum):
    TCP = 6
    UDP = 17


class TcpFlags(IntFlag):
    SYN = 0x02
    ACK = 0x10


class FlowEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    ts: float  # capture time, Unix epoch seconds
    src_ip: IPvAnyAddress
    dst_ip: IPvAnyAddress
    src_port: int = Field(ge=0, le=65535)
    dst_port: int = Field(ge=0, le=65535)
    proto: Proto
    tcp_flags: int = Field(default=0, ge=0, le=0xFF)
    pkt_len: int = Field(ge=0)

    def has_flag(self, flag: TcpFlags) -> bool:
        return bool(self.tcp_flags & flag)

    @property
    def partition_key(self) -> bytes:
        return str(self.src_ip).encode()

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> FlowEvent:
        return cls.model_validate_json(data)
