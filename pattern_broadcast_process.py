from pyflink.datastream import RuntimeContext, KeyedBroadcastProcessFunction, BroadcastProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common.typeinfo import Types
from pyflink.common import Row
from typing import Any

class PatternBroadcastProcessFunction(KeyedBroadcastProcessFunction):
    def __init__(self):
        self._prev_action_state_desc = ValueStateDescriptor(
            "last_action",
            Types.STRING()
        )
        self._pattern_desc = MapStateDescriptor(
            "pattern",
            Types.INT(),
            Types.OBJECT_ARRAY(Types.STRING())
        )
        self._prev_action_state = None

    def open(self, runtime_context: RuntimeContext):
        self._prev_action_state = runtime_context.get_state(self._prev_action_state_desc)

    def process_broadcast_element(self, value: Any, ctx: KeyedBroadcastProcessFunction.Context):
        ctx.get_broadcast_state(self._pattern_desc).put(1, value["pattern"])

    def process_element(self, value: Any, ctx: KeyedBroadcastProcessFunction.ReadOnlyContext):
        pattern = ctx.get_broadcast_state(self._pattern_desc).get(1)
        prev_action = self._prev_action_state.value()

        if pattern != None and prev_action != None:
            if pattern == [prev_action, value["action"]]:
                yield Row(value["id"], f"Pattern {str(pattern)} matched")
        
        self._prev_action_state.update(value[1])