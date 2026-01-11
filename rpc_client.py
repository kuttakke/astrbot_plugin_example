import asyncio
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

    async def call(
        self,
        *,
        module_id: str,
        method: str,
        params: BaseParameters,
        unified_msg_origin: str,
        resp_model: type[TResponse],
    ) -> TResponse:
        reader, writer = await asyncio.open_unix_connection(self.socket_path)  # type: ignore

        req = CallParameters(
            module_id=module_id,
            method=method,
            params=params,
            unified_msg_origin=unified_msg_origin,
        )

        packed = msgpack.packb(req.model_dump(), use_bin_type=True)
        writer.write(len(packed).to_bytes(4, "big"))  # type: ignore
        writer.write(packed)
        await writer.drain()

        size = int.from_bytes(await reader.readexactly(4), "big")
        data = msgpack.unpackb(await reader.readexactly(size), raw=False)

        resp = CallResponse(**data)
        writer.close()
        await writer.wait_closed()

        if not resp.ok:
            raise RuntimeError(resp.error_message)

        return resp_model.model_validate(resp.data)


__rpc_client_instance: RPCClient | None = None


def get_rpc_client(socket_path: Path = Path("/run/logic/logic.sock")) -> RPCClient:
    global __rpc_client_instance
    if __rpc_client_instance is None:
        __rpc_client_instance = RPCClient(socket_path)
    return __rpc_client_instance
