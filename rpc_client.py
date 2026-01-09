import asyncio
from pathlib import Path

import msgpack
from pydantic import BaseModel, Field


class CallRequest(BaseModel):
    method: str = Field(..., description="The name of the method to be called")
    params: dict = Field(
        default_factory=dict, description="Parameters for the method call"
    )
    unified_msg_origin: str = Field(
        ..., description="The origin of the unified message"
    )


class CallResponse(BaseModel):
    success: bool = Field(..., description="Indicates if the call was successful")
    data: dict = Field(
        default_factory=dict, description="The data returned from the method call"
    )
    error_message: str = Field(
        ..., description="Error message if the call was not successful"
    )


class Client:
    def __init__(self, socket_path="/run/logic/logic.sock"):
        self.socket_path = Path(socket_path)

    async def _read_msgpack(self, reader: asyncio.StreamReader) -> dict:
        size_data = await reader.readexactly(4)
        size = int.from_bytes(size_data, byteorder="big")
        data = await reader.readexactly(size)
        return msgpack.unpackb(data, raw=False)

    async def _write_msgpack(self, writer: asyncio.StreamWriter, message: dict):
        packed = msgpack.packb(message, use_bin_type=True)
        size = len(packed)  # type: ignore
        writer.write(size.to_bytes(4, byteorder="big"))
        writer.write(packed)  # type: ignore
        await writer.drain()

    async def call_method(
        self, method: str, params: dict, unified_msg_origin: str
    ) -> CallResponse:
        reader, writer = await asyncio.open_unix_connection(self.socket_path)  # type: ignore
        request = CallRequest(
            method=method, params=params, unified_msg_origin=unified_msg_origin
        )
        await self._write_msgpack(writer, request.dict())

        response_data = await self._read_msgpack(reader)
        writer.close()
        await writer.wait_closed()

        return CallResponse(**response_data)
