#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Binance Data Streaming to ParquetDataCatalog using Actor
#  此程式從Binance串流實時數據並寫入ParquetDataCatalog
# -------------------------------------------------------------------------------------------------
import asyncio
import logging
import os

from dotenv import load_dotenv
from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.config import (
    BinanceDataClientConfig,
)
from nautilus_trader.adapters.binance.factories import (
    BinanceLiveDataClientFactory,
)
from nautilus_trader.config import (
    CacheConfig,
    InstrumentProviderConfig,
    LoggingConfig,
    StreamingConfig,
    TradingNodeConfig,
)
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import (
    Bar,
    OrderBookDeltas,
    OrderBookDepth10,
    QuoteTick,
    TradeTick,
)
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.persistence.writer import RotationMode

from src.binance_collector import BinanceDataCollector, BinanceDataCollectorConfig

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)


async def main():
    """
    主程序函數。
    """
    # 載入環境變數
    load_dotenv()
    api_key = os.getenv("BINANCE_FUTURES_API_KEY")
    api_secret = os.getenv("BINANCE_FUTURES_API_SECRET")

    if not api_key or not api_secret:
        raise ValueError(
            "缺少Binance API憑證。請確保在.env文件中設置BINANCE_FUTURES_API_KEY和BINANCE_FUTURES_API_SECRET"
        )

    # 設置交易對和數據類型
    instrument_ids = [
        "ADAUSDT-PERP.BINANCE",
        "LTCUSDT-PERP.BINANCE",
        "SUIUSDT-PERP.BINANCE",
        "SOLUSDT-PERP.BINANCE",
        "XRPUSDT-PERP.BINANCE",
    ]

    # 設置K線類型
    bar_types = [
        f"{instrument_id}-1-MINUTE-LAST-EXTERNAL" for instrument_id in instrument_ids
    ]

    # 設置目錄路徑和其他參數
    catalog_path = "./data/catalog"
    is_testnet = False  # 使用測試網以防出錯
    duration_seconds = None  # 運行1小時，設為None則無限運行

    # 配置交易節點
    config_node = TradingNodeConfig(
        trader_id=TraderId("DATA-COLLECTOR-001"),
        logging=LoggingConfig(
            log_level="INFO",
            log_level_file="DEBUG",
            log_file_name="binance-data-collector.json",
            log_file_format="json",
            log_directory="./logs",
            log_colors=True,
            use_pyo3=True,
        ),
        cache=CacheConfig(
            timestamps_as_iso8601=True,
            flush_on_start=False,
        ),
        data_clients={
            "BINANCE": BinanceDataClientConfig(
                api_key=api_key,
                api_secret=api_secret,
                account_type=BinanceAccountType.USDT_FUTURE,
                testnet=is_testnet,
                instrument_provider=InstrumentProviderConfig(load_all=True),
            ),
        },
        streaming=StreamingConfig(
            catalog_path=catalog_path,
            include_types=[
                # Instrument,
                OrderBookDepth10,
                OrderBookDeltas,
                QuoteTick,
                TradeTick,
                Bar,
                # OrderBook,
            ],  # 包含要寫入的類型
            rotation_mode=RotationMode.SIZE,
            max_file_size=1024 * 1024 * 1024,  # 1GB
        ),
        timeout_connection=30.0,
        timeout_disconnection=10.0,
        timeout_post_stop=5.0,
    )

    # 配置數據串流Actor
    collector_config = BinanceDataCollectorConfig(
        instrument_ids=instrument_ids,
        bar_types=bar_types,
        catalog_path=catalog_path,
    )

    # 創建交易節點
    node = TradingNode(config=config_node)

    # 創建並添加數據串流Actor
    collector = BinanceDataCollector(config=collector_config)
    node.trader.add_actor(collector)

    # 註冊數據客戶端工廠
    node.add_data_client_factory("BINANCE", BinanceLiveDataClientFactory)

    # 構建節點
    node.build()

    try:
        # 啟動節點
        await node.run_async()

        # 如果指定了持續時間，則在指定時間後停止
        if duration_seconds is not None:
            log.info(f"數據串流將運行 {duration_seconds} 秒")
            await asyncio.sleep(duration_seconds)
            await node.stop_async()
        else:
            # 無限運行，直到收到鍵盤中斷
            log.info("數據串流無限運行中，按Ctrl+C停止")
            while True:
                await asyncio.sleep(60)

    except asyncio.CancelledError:
        log.info("數據串流被取消")
    except Exception as e:
        log.error(f"數據串流出錯: {e}")
    finally:
        # 停止節點並釋放資源
        await node.stop_async()
        await asyncio.sleep(1)  # 給予時間進行清理
        node.dispose()
        log.info("節點已停止，資源已釋放")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("程序被用戶中斷（檢測到Ctrl+C）...")
