from threading import Thread
from threading import Event
import concurrent.futures as concurent_f
from io import BytesIO
import subprocess
import os
from typing import List
from pyarrow import Schema, Table, csv, unify_schemas
from pyarrow import parquet as pa_pq
from time import sleep

import logging
logger = logging.getLogger(__name__)


class snmpDaemon(Thread):
    def __init__(self, snmp_agent_ip: str,
                 ask_interval: int, timeout_exit: int):
        self._stop_event = Event()
        self._initialized_schema: bool = False
        self._initialized_writers: bool = False
        self.snmp_agent_ip: str = snmp_agent_ip
        self.ask_interval: int = ask_interval
        self._check_exit_interval: int = timeout_exit - 2
        self.batch_download_size: int = 50
        self._timeout_snmp: int = 5
        self._timeout_snmp_subprocess: float = float(
            self._timeout_snmp * 2)
        self._if_table_oid: str = "1.3.6.1.2.1.2.2"
        self._if_x_table_oid: str = "1.3.6.1.2.1.31.1.1"
        self.writers = []
        Thread.__init__(self)

    def initialize_first_request(self) -> Schema:
        counts_list = []
        with concurent_f.ThreadPoolExecutor(max_workers=5) as executor:
            # Start the load operations and mark each future with its URL
            future_to_url = {
                executor.submit(
                    self._get_table, self._if_table_oid): "if_table",
                executor.submit(
                    self._get_table, self._if_x_table_oid): "if_x_table"
            }
            logger.debug("start wait treads")
            for future in concurent_f.as_completed(
                    future_to_url, timeout=15):
                table_name = future_to_url[future]
                try:
                    logger.debug('%r check this table' %
                                 table_name)
                    data = future.result()
                    logger.debug(
                        "Get resulted bytes success")
                except Exception as exc:
                    logger.error(
                        '%r generated an exception: %s' %
                        (table_name, exc))
                    raise Exception(
                        "Can't initialize daemon")
                else:
                    temp_table: Table = csv.read_csv(
                        data, read_options=csv.ReadOptions(
                            skip_rows=2))
                    if table_name == "if_table":
                        self.if_table_schema = temp_table.schema
                        counts_list.append(
                            temp_table.num_rows)
                    elif table_name == "if_x_table":
                        self.if_x_table_schema = temp_table.schema
                        counts_list.append(
                            temp_table.num_rows)

        self.schema = unify_schemas([
            self.if_table_schema,
            self.if_x_table_schema
        ])
        if not all(x == counts_list[0]
                   for x in counts_list):
            raise Exception(
                f"Не одинаковое количество интерфейсов в таблицах\n\
                        {counts_list}")
        self._count_interfaces = counts_list[0]
        self._initialized_schema = True
        return self.schema

    def initialize_writers(self, dirname: str) -> int:
        if self._initialized_schema == False:
            raise Exception(
                "Не инициализирована схема данных")
        try:
            os.mkdir(dirname)
        except FileExistsError as e:
            logger.info(f"Папка уже существует: {e}")
        for index in range(0, self._count_interfaces):
            self.writers.append(
                pa_pq.ParquetWriter(os.path.join(
                    dirname, f"interface_{index}.parquet"),
                    self.schema)
            )
        self._initialized_writers = True
        return self._count_interfaces

    def _get_table(self, table_oid: str) -> BytesIO:
        """
        Вход:
            table_oid: должен быть oid таблицы, иначе snmp вернёт ошибку
        """
        result: BytesIO = BytesIO()
        commands: List[str] = [
            "snmptable",
            "-t", str(self._timeout_snmp),
            "-v2c", "-c", "sec", "-Cf", ",",
            "-Cr", str(self.batch_download_size),
            self.snmp_agent_ip, table_oid
        ]
        # Выполняем команду в системном шелле
        try:
            process = subprocess.run(
                commands,
                check=True,
                timeout=self._timeout_snmp_subprocess,
                capture_output=True)
            result.write(process.stdout)
        except subprocess.CalledProcessError as e:
            raise Exception(
                f"Произошла ошибка при получении данных по snmp {e}")
        except subprocess.TimeoutExpired as e:
            raise Exception(
                f"Процесс опроса snmp завершён по таймауту {e}")
        result.seek(0)
        return result

    def shutdown(self):
        self._stop_event.set()
        for writer in self.writers:
            writer.close()
        logger.debug("Writers are closed")

    def run(self):
        timer: int = self.ask_interval
        temp_wait_interval: int = 0
        count_errors: int = 0
        while not self._stop_event.is_set() and count_errors < 10:
            # Блок выжидания
            sleep(temp_wait_interval)
            timer = timer + temp_wait_interval
            if timer < self.ask_interval - temp_wait_interval:
                continue
            elif timer < self.ask_interval:
                temp_wait_interval = self.ask_interval - timer
                continue
            else:
                timer = 0
                temp_wait_interval = self._check_exit_interval

            # Блок работы
            logger.debug("Process task")
            try:
                table: Table = self.request()
                self.store_results(table)

            except Exception as e:
                logger.error(f"Error occure: {e}")
                count_errors += 1

        if count_errors == 10:
            logger.error("Too many errors")
        if self._stop_event.is_set():
            logger.info(
                "KeyboardInterrupt detected, closing background thread. ")

    def request(self) -> Table:
        temp_tables = {}
        with concurent_f.ThreadPoolExecutor(max_workers=5) as executor:
            # Start the load operations and mark each future with its URL
            future_to_url = {
                executor.submit(
                    self._get_table, self._if_table_oid): "if_table",
                executor.submit(
                    self._get_table, self._if_x_table_oid): "if_x_table"
            }
            logger.debug("start wait requests treads")
            for future in concurent_f.as_completed(
                    future_to_url, timeout=15):
                table_name = future_to_url[future]
                try:
                    logger.debug('%r check this table' %
                                 table_name)
                    data = future.result()
                    logger.debug(
                        "Get resulted bytes success")
                except Exception as exc:
                    logger.error(
                        '%r generated an exception: %s' %
                        (table_name, exc))
                    raise Exception(
                        f"Error with request table {table_name}")
                else:
                    temp_tables.update(
                        {table_name: csv.read_csv(
                            data,
                            read_options=csv.ReadOptions(
                                skip_rows=2))
                         })
        out_table = temp_tables["if_table"]
        for name, col in zip(
                temp_tables["if_x_table"].column_names,
                temp_tables["if_x_table"].columns):
            out_table = out_table.append_column(name, col)

        return out_table

    def store_results(self, table: Table):
        batches = table.to_batches(1)
        if len(batches) != self._count_interfaces:
            raise Exception(
                "Неправильное количество батчей")
        for index, batch in enumerate(batches):
            self.writers[index].write_batch(batch)
        logger.info("data flushed")
