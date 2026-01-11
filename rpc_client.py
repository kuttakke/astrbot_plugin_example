import asyncio
import contextlib
from pathlib import Path
from typing import TypeVar

import msgpack
from pydantic import BaseModel, Field


class BaseParameters(BaseModel):
    """基础参数模型.

    所有 RPC 方法的参数模型都应该继承此类。

    Example:
        >>> class MyParams(BaseParameters):
        ...     name: str
        ...     value: int
    """


class CallParameters(BaseModel):
    """RPC 调用参数模型.

    Attributes:
        unified_msg_origin: 会话的唯一 ID 标识符，用于追踪请求/响应
        method: 要调用的方法名称，格式为 "模块名.方法名"
        params: 方法调用的参数，继承自 BaseParameters
    """

    module_id: str = Field(..., description="模块 ID")
    unified_msg_origin: str = Field(..., description="会话的唯一 ID 标识符")
    method: str = Field(..., description="要调用的方法名称")
    params: BaseParameters = Field(..., description="方法调用的参数")


class BaseResponse(BaseModel):
    """基础响应模型.

    所有 RPC 方法的响应模型都应该继承此类。
    """


TResponse = TypeVar("TResponse", bound=BaseResponse)


class CallResponse(BaseModel):
    """RPC 调用响应模型.

    Attributes:
        ok: 操作是否成功
        unified_msg_origin: 会话的唯一 ID 标识符，与请求对应
        data: 方法调用返回的数据，类型为 BaseResponse 或 None
        error_message: 错误信息，如果有的话
    """

    ok: bool = Field(..., description="操作是否成功")
    unified_msg_origin: str = Field(..., description="会话的唯一 ID 标识符")
    data: BaseResponse | None = Field(..., description="方法调用返回的数据")
    error_message: str = Field(..., description="错误信息，如果有的话")


class RPCClient:
    def __init__(self, socket_path: Path = Path("/run/logic/logic.sock")):
        self.socket_path = socket_path

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

        self._req_id: int = 0
        self._write_lock = asyncio.Lock()
        self._pending: dict[int, asyncio.Future] = {}

        self._reader_task: asyncio.Task | None = None

    async def connect(self):
        if self._reader is not None:
            return

        self._reader, self._writer = await asyncio.open_unix_connection(  # type: ignore
            self.socket_path
        )
        self._reader_task = asyncio.create_task(self._read_loop())

    def _reset_connection(self, exc: Exception | None = None):
        if self._writer:
            with contextlib.suppress(Exception):
                self._writer.close()
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(exc or RuntimeError("connection reset"))

        self._pending.clear()
        self._reader = None
        self._writer = None
        self._reader_task = None

    async def _read_loop(self):
        reader = self._reader
        assert reader is not None

        try:
            while True:
                req_id = int.from_bytes(await reader.readexactly(4), "big")
                size = int.from_bytes(await reader.readexactly(4), "big")
                data = msgpack.unpackb(
                    await reader.readexactly(size),
                    raw=False,
                )

                fut = self._pending.pop(req_id, None)
                if fut and not fut.done():
                    fut.set_result(data)

        except asyncio.IncompleteReadError as e:
            self._reset_connection(e)

    async def call(
        self,
        *,
        module_id: str,
        method: str,
        params: BaseParameters,
        unified_msg_origin: str,
        resp_model: type[TResponse],
    ) -> TResponse:
        for attempt in (1, 2):
            try:
                await self.connect()
                return await self._call_once(
                    module_id=module_id,
                    method=method,
                    params=params,
                    unified_msg_origin=unified_msg_origin,
                    resp_model=resp_model,
                )
            except (BrokenPipeError, RuntimeError) as e:
                self._reset_connection(e)
                if attempt == 2:
                    raise e
                await asyncio.sleep(5)
        raise RuntimeError("RPC server unavailable")

    async def _call_once(
        self,
        *,
        module_id: str,
        method: str,
        params: BaseParameters,
        unified_msg_origin: str,
        resp_model: type[TResponse],
    ) -> TResponse:
        await self.connect()

        self._req_id += 1
        req_id = self._req_id

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending[req_id] = future

        req = CallParameters(
            module_id=module_id,
            method=method,
            params=params,
            unified_msg_origin=unified_msg_origin,
        )

        payload = msgpack.packb(req.model_dump(), use_bin_type=True)

        async with self._write_lock:
            assert self._writer is not None
            self._writer.write(req_id.to_bytes(4, "big"))
            self._writer.write(len(payload).to_bytes(4, "big"))  # type: ignore
            self._writer.write(payload)  # type: ignore
            await self._writer.drain()

        data = await future

        resp = CallResponse(**data)
        if not resp.ok:
            raise RuntimeError(resp.error_message)

        return resp_model.model_validate(resp.data)


__rpc_client_instance: RPCClient | None = None


def get_rpc_client(socket_path: Path = Path("/run/logic/logic.sock")) -> RPCClient:
    global __rpc_client_instance
    if __rpc_client_instance is None:
        __rpc_client_instance = RPCClient(socket_path)
    return __rpc_client_instance
