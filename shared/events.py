"""Records passed between services over Kafka. All are keyed by source IP."""

from __future__ import annotations

from enum import IntEnum, IntFlag
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, IPvAnyAddress


class Proto(IntEnum):
    TCP = 6
    UDP = 17


class TcpFlags(IntFlag):
    SYN = 0x02
    ACK = 0x10


class Record(BaseModel):
    model_config = ConfigDict(frozen=True)

    src_ip: IPvAnyAddress

    @property
    def partition_key(self) -> bytes:
        return str(self.src_ip).encode()

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        return cls.model_validate_json(data)


class FlowEvent(Record):
    """One packet seen on the victim's interface."""

    ts: float  # capture time, Unix epoch seconds
    dst_ip: IPvAnyAddress
    src_port: int = Field(ge=0, le=65535)
    dst_port: int = Field(ge=0, le=65535)
    proto: Proto
    tcp_flags: int = Field(default=0, ge=0, le=0xFF)
    pkt_len: int = Field(ge=0)

    def has_flag(self, flag: TcpFlags) -> bool:
        return bool(self.tcp_flags & flag)


class FeatureVector(Record):
    """Features of one source over the window ending at `window_end` (exclusive, epoch seconds)."""

    dst_ip: IPvAnyAddress  # the destination this source sent the most packets to
    window_end: int
    pkt_rate: float
    byte_rate: float
    syn_ack_ratio: float
    dst_distinct_srcs: int
    dst_src_entropy: float
    dst_port_entropy: float


class Score(Record):
    dst_ip: IPvAnyAddress
    window_end: int
    detector: str
    score: float = Field(ge=0.0, le=1.0)  # attack probability
