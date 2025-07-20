# BSD 2-Clause License
#
# Copyright (c) 2024, ZhengYu, Xu
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import sys

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

class ArrowException(Exception): ...
class ArrowInvalid(ValueError, ArrowException): ...
class ArrowMemoryError(MemoryError, ArrowException): ...
class ArrowKeyError(KeyError, ArrowException): ...
class ArrowTypeError(TypeError, ArrowException): ...
class ArrowNotImplementedError(NotImplementedError, ArrowException): ...
class ArrowCapacityError(ArrowException): ...
class ArrowIndexError(IndexError, ArrowException): ...
class ArrowSerializationError(ArrowException): ...

class ArrowCancelled(ArrowException):
    signum: int | None
    def __init__(self, message: str, signum: int | None = None) -> None: ...

ArrowIOError = IOError

class StopToken: ...

def enable_signal_handlers(enable: bool) -> None: ...

have_signal_refcycle: bool

class SignalStopHandler:
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type, exc_value, exc_tb) -> None: ...
    def __dealloc__(self) -> None: ...
    @property
    def stop_token(self) -> StopToken: ...

__all__ = [
    "ArrowException",
    "ArrowInvalid",
    "ArrowMemoryError",
    "ArrowKeyError",
    "ArrowTypeError",
    "ArrowNotImplementedError",
    "ArrowCapacityError",
    "ArrowIndexError",
    "ArrowSerializationError",
    "ArrowCancelled",
    "ArrowIOError",
    "StopToken",
    "enable_signal_handlers",
    "have_signal_refcycle",
    "SignalStopHandler",
]
