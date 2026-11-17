"""M8 装配：模拟值班站、独立派单库、领域服务的单例与 FastAPI 依赖。"""
from __future__ import annotations

from ...config import Settings
from .persistence import DispatchStore
from .service import DispatchService
from .station import StationBackend
from .station_client import StationClient


class DispatchContext:
    """进程级单例：测试通过 create_app(settings) 获得全新的一套模拟环境。"""

    def __init__(self, settings: Settings):
        self.backend = StationBackend()
        self.client = StationClient(self.backend)
        self.store = DispatchStore(settings.dispatch_db_path)
        self.service = DispatchService(
            self.store, self.client,
            default_threshold=settings.rain_warn_threshold_mm,
            thresholds=settings.rain_warn_station_thresholds)


def get_context(request) -> DispatchContext:
    return request.app.state.dispatch_ctx
