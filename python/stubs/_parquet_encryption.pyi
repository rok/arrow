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

import datetime as dt

from typing import Callable

from ._parquet import FileDecryptionProperties, FileEncryptionProperties
from .lib import _Weakrefable

class EncryptionConfiguration(_Weakrefable):
    footer_key: str
    column_keys: dict[str, list[str]]
    encryption_algorithm: str
    plaintext_footer: bool
    double_wrapping: bool
    cache_lifetime: dt.timedelta
    internal_key_material: bool
    data_key_length_bits: int

    def __init__(
        self,
        footer_key: str,
        *,
        column_keys: dict[str, str | list[str]] | None = None,
        encryption_algorithm: str | None = None,
        plaintext_footer: bool | None = None,
        double_wrapping: bool | None = None,
        cache_lifetime: dt.timedelta | None = None,
        internal_key_material: bool | None = None,
        data_key_length_bits: int | None = None,
    ) -> None: ...

class DecryptionConfiguration(_Weakrefable):
    cache_lifetime: dt.timedelta
    def __init__(self, *, cache_lifetime: dt.timedelta | None = None): ...

class KmsConnectionConfig(_Weakrefable):
    kms_instance_id: str
    kms_instance_url: str
    key_access_token: str
    custom_kms_conf: dict[str, str]
    def __init__(
        self,
        *,
        kms_instance_id: str | None = None,
        kms_instance_url: str | None = None,
        key_access_token: str | None = None,
        custom_kms_conf: dict[str, str] | None = None,
    ) -> None: ...
    def refresh_key_access_token(self, value: str) -> None: ...

class KmsClient(_Weakrefable):
    def wrap_key(self, key_bytes: bytes, master_key_identifier: str) -> str: ...
    def unwrap_key(self, wrapped_key: str, master_key_identifier: str) -> str: ...

class CryptoFactory(_Weakrefable):
    def __init__(self, kms_client_factory: Callable[[KmsConnectionConfig], KmsClient]): ...
    def file_encryption_properties(
        self,
        kms_connection_config: KmsConnectionConfig,
        encryption_config: EncryptionConfiguration,
    ) -> FileEncryptionProperties: ...
    def file_decryption_properties(
        self,
        kms_connection_config: KmsConnectionConfig,
        decryption_config: DecryptionConfiguration | None = None,
    ) -> FileDecryptionProperties: ...
    def remove_cache_entries_for_token(self, access_token: str) -> None: ...
    def remove_cache_entries_for_all_tokens(self) -> None: ...
