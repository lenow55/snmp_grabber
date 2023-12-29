from threading import Thread
from threading import Event
import concurrent.futures as concurent_f
from io import BytesIO
import subprocess
from typing import List
from pyarrow import Table, csv, unify_schemas


class snmpDaemon(Thread):
    def __init__(self, snmp_agent_ip: str,
                 ask_interval: int):
        self._stop = Event()
        self._initialized: bool = False
        self.snmp_agent_ip: str = snmp_agent_ip
        self.ask_interval: int = ask_interval
        self.batch_download_size: int = 50
        self._timeout_snmp: int = 5
        self._timeout_snmp_subprocess: float = float(
            self._timeout_snmp * 2)
        self._if_table_oid: str = "1.3.6.1.2.1.2.2"
        self._if_x_table_oid: str = "1.3.6.1.2.1.31.1.1"

    def initialize_first_request(self):

        with concurent_f.ThreadPoolExecutor(max_workers=5) as executor:
            # Start the load operations and mark each future with its URL
            future_to_url = {
                executor.submit(
                    self._get_table, self._if_table_oid): "if_table",
                executor.submit(
                    self._get_table, self._if_x_table_oid): "if_x_table"
            }
            print("start wait treads")
            for future in concurent_f.as_completed(
                    future_to_url, timeout=15):
                table_name = future_to_url[future]
                try:
                    print('%r check this table' %
                          table_name)
                    data = future.result()
                    print("Get resulted bytes")
                except Exception as exc:
                    print(
                        '%r generated an exception: %s' %
                        (table_name, exc))
                    raise Exception(
                        "Can't initialize daemon")
                else:
                    print(data.getvalue())
                    temp_table: Table = csv.read_csv(
                        data,
                        read_options=csv.ReadOptions(skip_rows=2)
                    )
                    if table_name == "if_table":
                        self.if_table_schema = temp_table.schema
                    elif table_name == "if_x_table":
                        self.if_x_table_schema = temp_table.schema

            self.schema = unify_schemas(
                self.if_table_schema,
                self.if_x_table_schema)
            self._initialized = True
            print("Result schema")
            print(self.schema)

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
