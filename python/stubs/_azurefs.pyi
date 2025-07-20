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

from typing import Literal

from ._fs import FileSystem

class AzureFileSystem(FileSystem):
    """
    Azure Blob Storage backed FileSystem implementation

    This implementation supports flat namespace and hierarchical namespace (HNS) a.k.a.
    Data Lake Gen2 storage accounts. HNS will be automatically detected and HNS specific
    features will be used when they provide a performance advantage. Azurite emulator is
    also supported. Note: `/` is the only supported delimiter.

    The storage account is considered the root of the filesystem. When enabled, containers
    will be created or deleted during relevant directory operations. Obviously, this also
    requires authentication with the additional permissions.

    By default `DefaultAzureCredential <https://github.com/Azure/azure-sdk-for-cpp/blob/main/sdk/identity/azure-identity/README.md#defaultazurecredential>`__
    is used for authentication. This means it will try several types of authentication
    and go with the first one that works. If any authentication parameters are provided when
    initialising the FileSystem, they will be used instead of the default credential.

    Parameters
    ----------
    account_name : str
        Azure Blob Storage account name. This is the globally unique identifier for the
        storage account.
    account_key : str, default None
        Account key of the storage account. If sas_token and account_key are None the
        default credential will be used. The parameters account_key and sas_token are
        mutually exclusive.
    blob_storage_authority : str, default None
        hostname[:port] of the Blob Service. Defaults to `.blob.core.windows.net`. Useful
        for connecting to a local emulator, like Azurite.
    dfs_storage_authority : str, default None
        hostname[:port] of the Data Lake Gen 2 Service. Defaults to
        `.dfs.core.windows.net`. Useful for connecting to a local emulator, like Azurite.
    blob_storage_scheme : str, default None
        Either `http` or `https`. Defaults to `https`. Useful for connecting to a local
        emulator, like Azurite.
    dfs_storage_scheme : str, default None
        Either `http` or `https`. Defaults to `https`. Useful for connecting to a local
        emulator, like Azurite.
    sas_token : str, default None
        SAS token for the storage account, used as an alternative to account_key. If sas_token
        and account_key are None the default credential will be used. The parameters
        account_key and sas_token are mutually exclusive.

    Examples
    --------
    >>> from pyarrow import fs
    >>> azure_fs = fs.AzureFileSystem(account_name="myaccount")
    >>> azurite_fs = fs.AzureFileSystem(
    ...     account_name="devstoreaccount1",
    ...     account_key="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
    ...     blob_storage_authority="127.0.0.1:10000",
    ...     dfs_storage_authority="127.0.0.1:10000",
    ...     blob_storage_scheme="http",
    ...     dfs_storage_scheme="http",
    ... )

    For usage of the methods see examples for :func:`~pyarrow.fs.LocalFileSystem`.
    """

    def __init__(
        self,
        account_name: str,
        account_key: str | None = None,
        blob_storage_authority: str | None = None,
        dfs_storage_authority: str | None = None,
        blob_storage_schema: Literal["http", "https"] = "https",
        dfs_storage_schema: Literal["http", "https"] = "https",
        sas_token: str | None = None,
    ) -> None: ...
