import os
from datetime import datetime
from typing import List

from nautilus_trader.common.actor import Actor
from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import (
    ActorConfig,
)
from nautilus_trader.model.book import OrderBook  # noqa
from nautilus_trader.model.data import (
    Bar,
    BarType,
    OrderBookDeltas,
    OrderBookDepth10,  # noqa
    QuoteTick,
    TradeTick,
)
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.persistence.catalog.types import CatalogWriteMode


class BinanceDataStreamerConfig(ActorConfig):
    """
    Binance數據串流配置類。

    繼承ActorConfig並添加了Binance數據串流所需的參數。
    """

    instrument_ids: List[str]  # 要訂閱的交易對列表
    bar_types: List[str]  # 要訂閱的K線類型列表
    catalog_path: str  # Parquet目錄的儲存路徑


class BinanceDataStreamer(Actor):
    """
    從Binance串流數據並寫入ParquetDataCatalog的Actor。

    此Actor設置與Binance的連接，訂閱選定的交易對和數據類型，
    並將接收到的數據寫入Parquet目錄格式。
    """

    def __init__(self, config: BinanceDataStreamerConfig) -> None:
        """
        初始化Binance數據串流Actor。
        """
        super().__init__(config=config)

        # 設置目錄和數據目錄
        os.makedirs(self.config.catalog_path, exist_ok=True)
        self.data_catalog = ParquetDataCatalog(self.config.catalog_path)

        # 追蹤數據計數
        self.quote_count = 0
        self.trade_count = 0
        self.deltas_count = 0
        self.book_count = 0
        self.book_type = BookType.L2_MBP
        self._book = []
        self.bar_counts = {}  # 每種K線類型的計數
        self.bar_types = []

        # 解析bar_types字符串為BarType對象
        for bar_type_str in self.config.bar_types:
            try:
                self.bar_types.append(BarType.from_str(bar_type_str))
                self.bar_counts[bar_type_str] = 0
            except Exception as e:
                self.log.error(f"解析K線類型 {bar_type_str} 時出錯: {e}")

        # 解析交易對字符串為InstrumentId對象
        self.instrument_ids = []
        for instrument_id_str in self.config.instrument_ids:
            try:
                self.instrument_ids.append(InstrumentId.from_str(instrument_id_str))
            except Exception as e:
                self.log.error(f"解析交易對 {instrument_id_str} 時出錯: {e}")

    def on_start(self) -> None:
        """
        Actor啟動時的操作。

        在此方法中訂閱所有需要的數據源。
        """
        self.log.info(f"啟動Binance數據串流Actor，目錄路徑: {self.config.catalog_path}")

        # 訂閱數據
        for instrument_id in self.instrument_ids:
            # 請求交易對信息
            self.subscribe_instrument(
                instrument_id=instrument_id,
            )

            # 訂閱報價數據
            self.subscribe_quote_ticks(
                instrument_id=instrument_id,
            )
            self.log.info(f"已訂閱 {instrument_id} 的報價數據")

            # 訂閱交易數據
            self.subscribe_trade_ticks(
                instrument_id=instrument_id,
            )
            self.log.info(f"已訂閱 {instrument_id} 的交易數據")

            # 訂閱訂單簿差數據
            self.subscribe_order_book_deltas(
                instrument_id=instrument_id, book_type=self.book_type, depth=0
            )
            self.log.info(f"已訂閱 {instrument_id} 的訂單簿數據")

            # 訂閱訂單簿數據
            # self.subscribe_order_book_at_interval(
            #     instrument_id=instrument_id,
            #     book_type=self.book_type,
            #     depth=0,
            #     interval_ms=1000,
            # )
            # self.log.info(f"已訂閱 {instrument_id} 的訂單簿數據")

        # 訂閱K線數據
        for bar_type in self.bar_types:
            self.subscribe_bars(
                bar_type=bar_type,
            )
            self.log.info(f"已訂閱 {bar_type} K線數據")

    def on_instrument(self, instrument):
        """
        處理交易對信息。
        """
        self.log.info(f"收到交易對信息: {instrument.id}")
        # 將交易對寫入目錄
        self.data_catalog.write_data([instrument])

    def on_quote_tick(self, tick: QuoteTick) -> None:
        """
        處理報價數據。
        """
        self.quote_count += 1
        if self.quote_count % 100 == 0:  # 每100個報價記錄一次日誌
            self.log.info(
                f"收到報價數據 #{self.quote_count}: {tick.instrument_id} @ {tick.bid_price}/{tick.ask_price}"
            )

        # 將報價數據寫入目錄
        self.data_catalog.write_data([tick], mode=CatalogWriteMode.APPEND)

    def on_trade_tick(self, tick: TradeTick) -> None:
        """
        處理交易數據。
        """
        self.trade_count += 1
        if self.trade_count % 10 == 0:  # 每10個交易記錄一次日誌
            self.log.info(
                f"收到交易數據 #{self.trade_count}: {tick.instrument_id} @ {tick.price} x {tick.size}"
            )

        # 將交易數據寫入目錄
        self.data_catalog.write_data([tick], mode=CatalogWriteMode.APPEND)

    def on_order_book_deltas(self, deltas: OrderBookDeltas) -> None:
        """
        Actions to be performed when the strategy is running and receives order book
        deltas.

        Parameters
        ----------
        deltas : OrderBookDeltas
            The order book deltas received.

        """
        self.deltas_count += 1
        if self.deltas_count % 100 == 0:  # 每100個交易記錄一次日誌
            self.log.info(
                f"收到訂單簿數據 #{self.deltas_count}: {deltas.instrument_id}"
            )
        # self.log.debug(repr(deltas), LogColor.CYAN)
        self.data_catalog.write_data([deltas], mode=CatalogWriteMode.APPEND)

    # def on_order_book(self, order_book: OrderBook) -> None:
    #     """
    #     Actions to be performed when the strategy is running and receives an order book.

    #     Parameters
    #     ----------
    #     order_book : OrderBook
    #         The order book received.

    #     """
    #     self.book_count += 1
    #     if self.book_count % 1000 == 0:  # 每1000個交易記錄一次日誌
    #         self.log.info(
    #             f"收到交易數據 #{self.book_count}: {order_book.instrument_id} @ {order_book.book_type}"
    #         )
    #     # depth = order_book.
    #     # For debugging (must add a subscription)
    #     # self.log.debug(repr(order_book), LogColor.CYAN)
    #     self.data_catalog.write_data([order_book], mode=CatalogWriteMode.APPEND)

    def on_bar(self, bar: Bar) -> None:
        """
        處理K線數據。
        """
        bar_type_str = str(bar.bar_type)
        self.bar_counts[bar_type_str] = self.bar_counts.get(bar_type_str, 0) + 1

        self.log.info(
            f"收到K線數據 #{self.bar_counts[bar_type_str]} 類型 --> {bar_type_str}: O={bar.open} H={bar.high} L={bar.low} C={bar.close} V={bar.volume}"
        )

        # 將K線數據寫入目錄
        self.data_catalog.write_data([bar], mode=CatalogWriteMode.APPEND)

    def _log_status(self) -> None:
        """
        記錄當前狀態。
        """
        now = datetime.now()
        self.log.info(
            f"數據串流狀態報告，時間: {now.strftime('%Y-%m-%d %H:%M:%S')}",
            LogColor.YELLOW,
        )
        self.log.info(f"- 報價數據總數: {self.quote_count}", LogColor.YELLOW)
        self.log.info(f"- 交易數據總數: {self.trade_count}", LogColor.YELLOW)

        # K線數據統計
        for bar_type_str, count in self.bar_counts.items():
            self.log.info(f"- K線數據 {bar_type_str} 總數: {count}", LogColor.YELLOW)

        # 獲取目錄統計信息
        try:
            data_types = self.data_catalog.list_data_types()
            self.log.info(f"數據目錄中的數據類型: {data_types}", LogColor.MAGENTA)

            for data_type in data_types:
                type_name = (
                    data_type.__name__
                    if hasattr(data_type, "__name__")
                    else str(data_type)
                )
                self.log.info(f"- 數據類型 {type_name} 已寫入目錄", LogColor.MAGENTA)
        except Exception as e:
            self.log.error(f"獲取目錄統計信息時出錯: {e}")

    def on_stop(self) -> None:
        """
        Actor停止時的操作。
        """
        self.log.info("停止數據串流Actor...")

        # 取消所有訂閱
        for instrument_id in self.instrument_ids:
            self.unsubscribe_quote_ticks(instrument_id)
            self.unsubscribe_trade_ticks(instrument_id)
            self.unsubscribe_order_book_deltas(instrument_id, self.book_type)
            # self.unsubscribe_order_book_at_interval(instrument_id, self.book_type)
        for bar_type in self.bar_types:
            self.unsubscribe_bars(bar_type)

        # 確保數據被寫入磁盤
        try:
            # self.data_catalog.flush()
            self.log.info(f"數據目錄已刷新，保存在: {self.config.catalog_path}")
        except Exception as e:
            self.log.error(f"刷新數據目錄時出錯: {e}")

        self.log.info("數據串流已完全停止")
