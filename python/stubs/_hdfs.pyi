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

from _typeshed import StrPath

from ._fs import FileSystem

class HadoopFileSystem(FileSystem):
    """
    HDFS backed FileSystem implementation

    Parameters
    ----------
    host : str
        HDFS host to connect to. Set to "default" for fs.defaultFS from
        core-site.xml.
    port : int, default 8020
        HDFS port to connect to. Set to 0 for default or logical (HA) nodes.
    user : str, default None
        Username when connecting to HDFS; None implies login user.
    replication : int, default 3
        Number of copies each block will have.
    buffer_size : int, default 0
        If 0, no buffering will happen otherwise the size of the temporary read
        and write buffer.
    default_block_size : int, default None
        None means the default configuration for HDFS, a typical block size is
        128 MB.
    kerb_ticket : string or path, default None
        If not None, the path to the Kerberos ticket cache.
    extra_conf : dict, default None
        Extra key/value pairs for configuration; will override any
        hdfs-site.xml properties.

    Examples
    --------
    >>> from pyarrow import fs
    >>> hdfs = fs.HadoopFileSystem(
    ...     host, port, user=user, kerb_ticket=ticket_cache_path
    ... )  # doctest: +SKIP

    For usage of the methods see examples for :func:`~pyarrow.fs.LocalFileSystem`.
    """
    def __init__(
        self,
        host: str,
        port: int = 8020,
        *,
        user: str | None = None,
        replication: int = 3,
        buffer_size: int = 0,
        default_block_size: int | None = None,
        kerb_ticket: StrPath | None = None,
        extra_conf: dict | None = None,
    ): ...
    @staticmethod
    def from_uri(uri: str) -> HadoopFileSystem:  # type: ignore[override]
        """
        Instantiate HadoopFileSystem object from an URI string.

        The following two calls are equivalent

        * ``HadoopFileSystem.from_uri('hdfs://localhost:8020/?user=test\
&replication=1')``
        * ``HadoopFileSystem('localhost', port=8020, user='test', \
replication=1)``

        Parameters
        ----------
        uri : str
            A string URI describing the connection to HDFS.
            In order to change the user, replication, buffer_size or
            default_block_size pass the values as query parts.

        Returns
        -------
        HadoopFileSystem
        """
