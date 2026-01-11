from astrbot.api.event import AstrMessageEvent

from .rpc_client import BaseParameters, BaseResponse, CallResponse, get_rpc_client


# ------------------- TestModule -------------------
class TestParameters(BaseParameters):
    value: int


class TestResponse(BaseResponse):
    result: int


class Testmodule:
    module_id = "test_module"

    @classmethod
    async def test_function(
        cls, params: TestParameters, *, event: AstrMessageEvent
    ) -> CallResponse[TestResponse]:
        client = get_rpc_client()
        return await client.call(
            module_id=cls.module_id,
            method="test_function",
            params=params,
            unified_msg_origin=event.unified_msg_origin,
            resp_model=TestResponse,
        )

    @classmethod
    async def test_function2(
        cls, params: TestParameters, *, event: AstrMessageEvent
    ) -> CallResponse[TestResponse]:
        client = get_rpc_client()
        return await client.call(
            module_id=cls.module_id,
            method="test_function2",
            params=params,
            unified_msg_origin=event.unified_msg_origin,
            resp_model=TestResponse,
        )
