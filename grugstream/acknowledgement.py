from enum import Enum


class Acknowledgement(str, Enum):
    ok = "ok"
    stop = "stop"
