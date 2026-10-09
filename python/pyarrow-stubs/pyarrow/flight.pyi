# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import re
import pyarrow
from typing import Any, Callable, IO, Iterable, Iterator, Literal, Mapping, NamedTuple, Self, Sequence
from _typeshed import Incomplete
from pyarrow._flight import Action as Action, ActionType as ActionType, BasicAuth as BasicAuth, CallInfo as CallInfo, CertKeyPair as CertKeyPair, ClientAuthHandler as ClientAuthHandler, ClientMiddleware as ClientMiddleware, ClientMiddlewareFactory as ClientMiddlewareFactory, DescriptorType as DescriptorType, FlightCallOptions as FlightCallOptions, FlightCancelledError as FlightCancelledError, FlightClient as FlightClient, FlightDataStream as FlightDataStream, FlightDescriptor as FlightDescriptor, FlightEndpoint as FlightEndpoint, FlightError as FlightError, FlightInfo as FlightInfo, FlightInternalError as FlightInternalError, FlightMetadataReader as FlightMetadataReader, FlightMetadataWriter as FlightMetadataWriter, FlightMethod as FlightMethod, FlightServerBase as FlightServerBase, FlightServerError as FlightServerError, FlightStreamChunk as FlightStreamChunk, FlightStreamReader as FlightStreamReader, FlightStreamWriter as FlightStreamWriter, FlightTimedOutError as FlightTimedOutError, FlightUnauthenticatedError as FlightUnauthenticatedError, FlightUnauthorizedError as FlightUnauthorizedError, FlightUnavailableError as FlightUnavailableError, FlightWriteSizeExceededError as FlightWriteSizeExceededError, GeneratorStream as GeneratorStream, Location as Location, MetadataRecordBatchReader as MetadataRecordBatchReader, MetadataRecordBatchWriter as MetadataRecordBatchWriter, RecordBatchStream as RecordBatchStream, Result as Result, SchemaResult as SchemaResult, ServerAuthHandler as ServerAuthHandler, ServerCallContext as ServerCallContext, ServerMiddleware as ServerMiddleware, ServerMiddlewareFactory as ServerMiddlewareFactory, Ticket as Ticket, TracingServerMiddlewareFactory as TracingServerMiddlewareFactory, connect as connect
